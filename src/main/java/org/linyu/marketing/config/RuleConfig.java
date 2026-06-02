package org.linyu.marketing.config;

public class RuleConfig {

    /**
     * 加购后 30 分钟未支付
     */
    public static final long ABANDON_CART_DELAY_MS = 30 * 60 * 1000L;

    /**
     * 1 小时内重复加购窗口
     */
    public static final long REPEAT_CART_WINDOW_MS = 60 * 60 * 1000L;

    /**
     * 1 小时内同一 sku 加购达到 3 次
     */
    public static final int REPEAT_CART_THRESHOLD = 3;

    /**
     * 同一用户同一规则 7 天只能触达一次
     */
    public static final long USER_RULE_INTERVAL_MS = 7L * 24 * 60 * 60 * 1000L;

    /**
     * 同一用户同一 sku 同一规则 30 天只能触达一次
     */
    public static final long USER_SKU_RULE_INTERVAL_MS = 30L * 24 * 60 * 60 * 1000L;

    /**
     * 同一用户一天最多触达 3 次
     */
    public static final int USER_DAILY_MAX_PUSH = 3;

    /**
     * 7 天内触发加购未支付超过 5 次，认为可能是薅券用户
     */
    public static final int ABANDON_CART_RISK_THRESHOLD_7D = 5;
}