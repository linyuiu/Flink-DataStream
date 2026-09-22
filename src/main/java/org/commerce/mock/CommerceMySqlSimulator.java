package org.commerce.mock;

import org.commerce.config.CommerceConfig;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** 有限批次的 MySQL 业务模拟器：全进程统一控速，每个工作线程独占一个 JDBC 连接。 */
public final class CommerceMySqlSimulator {
    public static void main(String[] args) throws Exception {
        CommerceConfig config = CommerceConfig.load(args);
        MockOrderPlans orderPlans = new MockOrderPlans(config);

        int workerCount = Math.toIntExact(config.positive("mock.workers"));
        long orderCount = config.positive("mock.orders");
        long orderIntervalNanos = 1_000_000_000L / config.positive("mock.orders.per.second");
        if (orderIntervalNanos == 0) {
            throw new IllegalArgumentException("Rate exceeds simulator clock precision");
        }

        // 同一订单序号只由一个线程领取；所有线程共用一个发单节拍，避免速率乘以线程数。
        AtomicLong nextOrderIndex = new AtomicLong();
        AtomicLong nextPermitNanos = new AtomicLong(System.nanoTime());
        LongAdder completedOrders = new LongAdder();
        LongAdder emittedEvents = new LongAdder();
        try (MySqlBusinessWriter writer = new MySqlBusinessWriter(config)) {
            writer.initializeCatalog();
        }

        ExecutorService workers = Executors.newFixedThreadPool(workerCount);
        CompletionService<Void> completion = new ExecutorCompletionService<>(workers);
        List<Future<Void>> tasks = new ArrayList<>();
        long startedAtNanos = System.nanoTime();
        try {
            for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                tasks.add(
                        completion.submit(
                                () -> {
                                    try (MySqlBusinessWriter writer =
                                            new MySqlBusinessWriter(config)) {
                                        for (long orderIndex;
                                                (orderIndex = nextOrderIndex.getAndIncrement())
                                                        < orderCount; ) {
                                            checkInterrupted();
                                            awaitOrderPermit(nextPermitNanos, orderIntervalNanos);
                                            List<TransactionPlan> plans =
                                                    orderPlans.generate(
                                                            orderIndex, System.currentTimeMillis());
                                            writeOrder(writer, plans, emittedEvents);

                                            completedOrders.increment();
                                            if (completedOrders.sum() % 1000 == 0) {
                                                System.out.println(
                                                        "orders="
                                                                + completedOrders.sum()
                                                                + " outbox_events="
                                                                + emittedEvents.sum());
                                            }
                                        }
                                    }
                                    return null;
                                }));
            }
            // 按完成顺序收集结果；任何 worker 失败都会尽快传播，随后停止其他 worker。
            for (int finished = 0; finished < workerCount; finished++) {
                completion.take().get();
            }
        } finally {
            for (Future<Void> task : tasks) {
                if (!task.isDone()) {
                    task.cancel(true);
                }
            }
            workers.shutdownNow();
            workers.awaitTermination(10, TimeUnit.SECONDS);
        }

        double elapsedSeconds = (System.nanoTime() - startedAtNanos) / 1e9;
        System.out.printf(
                Locale.ROOT,
                "Completed orders=%d events=%d seconds=%.2f actual_orders_per_second=%.2f%n",
                completedOrders.sum(),
                emittedEvents.sum(),
                elapsedSeconds,
                completedOrders.sum() / elapsedSeconds);
    }

    private static void awaitOrderPermit(AtomicLong nextPermitNanos, long orderIntervalNanos)
            throws InterruptedException {
        long waitNanos = nextPermitNanos.getAndAdd(orderIntervalNanos) - System.nanoTime();
        if (waitNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(waitNanos);
        }
    }

    private static void writeOrder(
            MySqlBusinessWriter writer, List<TransactionPlan> plans, LongAdder emittedEvents)
            throws Exception {
        // 一张订单经历多个业务动作，每个动作单独提交，不把整个生命周期装进一个大事务。
        for (TransactionPlan plan : plans) {
            checkInterrupted();
            writer.execute(plan);
            if (plan.event != null) {
                emittedEvents.increment();
            }
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }
}
