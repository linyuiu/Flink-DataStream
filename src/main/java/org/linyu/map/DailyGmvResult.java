package org.linyu.map;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 写入 Doris 的今日 GMV 快照行。
 */
public class DailyGmvResult implements Serializable {
    public String dt;
    public String stat_time;
    public BigDecimal gmv;
    public long paid_order_count;
    public String update_time;
}
