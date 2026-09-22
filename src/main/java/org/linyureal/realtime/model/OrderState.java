package org.linyureal.realtime.model;

import java.util.*;

public class OrderState {
    public Map<String, String> rows = new LinkedHashMap<>();
    public Map<String, String> emitted = new LinkedHashMap<>();
    public OrderState() {}
    public OrderState(OrderState old) { rows.putAll(old.rows); emitted.putAll(old.emitted); }
}
