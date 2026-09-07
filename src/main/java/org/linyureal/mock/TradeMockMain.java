package org.linyureal.mock;

import org.linyureal.config.AppConfig;
import org.linyureal.model.cdc.Change;
import org.linyureal.mock.generator.TradeGenerator;
import org.linyureal.mock.scenario.Scenario;
import org.linyureal.mock.writer.*;
import java.time.*;
import java.util.*;

/** Standalone Java producer, not a Flink source. Bounded by mock.order.count. */
public final class TradeMockMain {
    public static void main(String[] args) throws Exception {
        AppConfig c=AppConfig.load(args);
        String mode=c.get("mock.mode");
        if(!Arrays.asList("kafka","mysql").contains(mode)) throw new IllegalArgumentException("mock.mode must be kafka/mysql");
        double duplicate=Double.parseDouble(c.get("mock.duplicate.rate")), disorder=Double.parseDouble(c.get("mock.shuffle.rate"));
        if(!Double.isFinite(duplicate) || !Double.isFinite(disorder) || duplicate<0 || duplicate>1 || disorder<0 || disorder>1)
            throw new IllegalArgumentException("Rates must be in [0,1]");
        Random random=new Random(Long.parseLong(c.get("mock.seed")));
        long count=c.positive("mock.order.count"), interval=1_000_000_000L/c.positive("mock.orders.per.second");
        String run=c.get("mock.run.id");
        if(!run.matches("[A-Za-z0-9_-]{1,30}")) throw new IllegalArgumentException("Invalid run ID");
        try(TradeWriter writer=mode.equals("mysql")?new MySqlTradeWriter(c):new KafkaTradeWriter(c)) {
            if(mode.equals("kafka")) writer.write(org.linyureal.mock.generator.DimensionGenerator.seed());
            for(long i=0;i<count;i++) {
                long start=System.nanoTime();
                Scenario s=Scenario.values()[(int)(i%Scenario.values().length)];
                // All lifecycle business times are in the past; CROSS_DAY_REFUND straddles dates.
                LocalDateTime base=LocalDateTime.now(ZoneId.of("Asia/Shanghai")).minusDays(2).withHour(10).withMinute(0).withSecond(0).withNano(0);
                List<List<Change>> txs=TradeGenerator.generate(run+"-"+i,s,base,random);
                if(mode.equals("kafka")) {
                    List<Change> messages=new ArrayList<>(); txs.forEach(messages::addAll);
                    if(random.nextDouble()<disorder) Collections.shuffle(messages,random);
                    for(Change change:messages) {
                        writer.write(Collections.singletonList(change));
                        if(random.nextDouble()<duplicate) writer.write(Collections.singletonList(change));
                    }
                } else for(List<Change> tx:txs) writer.write(tx);
                System.out.println("order="+run+"-"+i+" scenario="+s+" mode="+mode);
                long wait=interval-(System.nanoTime()-start);
                if(wait>0) java.util.concurrent.TimeUnit.NANOSECONDS.sleep(wait);
            }
        }
    }
}
