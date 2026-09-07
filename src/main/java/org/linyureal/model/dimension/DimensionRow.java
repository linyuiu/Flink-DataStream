package org.linyureal.model.dimension;

import java.io.Serializable;

public class DimensionRow implements Serializable {
    public String dimension_type, dimension_id, display_name, parent_id;
    public long version;
    public DimensionRow() {}
    public String key() { return dimension_type+":"+dimension_id; }
}
