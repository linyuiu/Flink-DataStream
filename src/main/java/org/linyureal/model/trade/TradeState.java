package org.linyureal.model.trade;

import java.io.Serializable;
import java.util.*;

/** Per-order persisted rows and emitted ledger projections. No automatic TTL:
 * expiry without an archival/bootstrap protocol would silently duplicate money. */
public class TradeState implements Serializable {
    public Map<String,String> rows = new LinkedHashMap<>();
    public Map<String,String> emitted = new LinkedHashMap<>();
    public TradeState() {}
    public TradeState(TradeState other) {
        rows.putAll(other.rows); emitted.putAll(other.emitted);
    }
}
