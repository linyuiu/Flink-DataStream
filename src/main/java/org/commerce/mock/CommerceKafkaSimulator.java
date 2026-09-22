package org.commerce.mock;

import org.commerce.config.CommerceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** 将现有 MySQL 模拟器的业务计划直接发布为 Kafka ODS 消息，用于跳过 MySQL 和 CDC 的链路测试。 */
public final class CommerceKafkaSimulator {
    private CommerceKafkaSimulator() {}

    public static void main(String[] args) throws Exception {
        CommerceConfig config = CommerceConfig.load(args);
        MockOrderPlans orderPlans = new MockOrderPlans(config);
        int workerCount = Math.toIntExact(config.positive("mock.workers"));
        long orderCount = config.positive("mock.orders");
        long orderIntervalNanos = 1_000_000_000L / config.positive("mock.orders.per.second");
        if (orderIntervalNanos == 0) {
            throw new IllegalArgumentException("Rate exceeds simulator clock precision");
        }

        // 维度 Topic 使用与 MySQL 初始化完全相同的目录数据。
        try (KafkaMockPublisher catalogPublisher = new KafkaMockPublisher(config, "catalog")) {
            catalogPublisher.publishCatalog();
        }

        AtomicLong nextOrderIndex = new AtomicLong();
        AtomicLong nextPermitNanos = new AtomicLong(System.nanoTime());
        LongAdder completedOrders = new LongAdder();
        LongAdder publishedEvents = new LongAdder();
        ExecutorService workers = Executors.newFixedThreadPool(workerCount);
        CompletionService<Void> completion = new ExecutorCompletionService<>(workers);
        List<Future<Void>> tasks = new ArrayList<>();
        long startedAtNanos = System.nanoTime();
        try {
            for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                final int publisherIndex = workerIndex;
                tasks.add(
                        completion.submit(
                                () -> {
                                    try (KafkaMockPublisher publisher =
                                            new KafkaMockPublisher(
                                                    config, "worker-" + publisherIndex)) {
                                        for (long orderIndex;
                                                (orderIndex = nextOrderIndex.getAndIncrement())
                                                        < orderCount; ) {
                                            checkInterrupted();
                                            awaitOrderPermit(nextPermitNanos, orderIntervalNanos);
                                            List<TransactionPlan> plans =
                                                    orderPlans.generate(
                                                            orderIndex, System.currentTimeMillis());
                                            publishedEvents.add(publisher.publishOrder(plans));
                                            completedOrders.increment();
                                            if (completedOrders.sum() % 1000 == 0) {
                                                System.out.println(
                                                        "orders="
                                                                + completedOrders.sum()
                                                                + " outbox_events="
                                                                + publishedEvents.sum());
                                            }
                                        }
                                    }
                                    return null;
                                }));
            }
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
                "Published orders=%d events=%d seconds=%.2f actual_orders_per_second=%.2f%n",
                completedOrders.sum(),
                publishedEvents.sum(),
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

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }
}
