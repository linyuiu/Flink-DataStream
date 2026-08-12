package org.linyu.config;

public final class GmvPolicy {

    private static final long serialVersionUID = 1L;
    private final boolean deductRefunds;

    public GmvPolicy(boolean deductRefunds) {
        this.deductRefunds = deductRefunds;
    }
    public boolean isDeductRefunds() {
        return deductRefunds;
    }
}
