package org.linyureal.mock.generator;

import org.linyureal.common.Json;
import org.linyureal.model.cdc.Change;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;

public final class DimensionGenerator {
    private DimensionGenerator() {}
    public static List<Change> seed() {
        List<Change> rows=new ArrayList<>();
        add(rows,"dim_product","P1","运动水杯","0",null); add(rows,"dim_product","P2","棉质T恤","0",null);
        add(rows,"dim_sku","SKU1","蓝色水杯","P1",null); add(rows,"dim_sku","SKU2","白色T恤M码","P2",null);
        add(rows,"dim_category","C0","全部品类","0",null);
        add(rows,"dim_category","C1","水杯","C0",null); add(rows,"dim_category","C2","服装","C0",null);
        add(rows,"dim_brand","B1","示例运动品牌","0",null); add(rows,"dim_brand","B2","示例服饰品牌","0",null);
        add(rows,"dim_shop","S1","杭州店","0",null); add(rows,"dim_shop","S2","上海店","0",null);
        add(rows,"dim_region","310000","上海市","0","PROVINCE");
        add(rows,"dim_region","310100","上海市","310000","CITY");
        add(rows,"dim_region","310115","浦东新区","310100","DISTRICT");
        add(rows,"dim_region","330000","浙江省","0","PROVINCE");
        add(rows,"dim_region","330100","杭州市","330000","CITY");
        add(rows,"dim_region","330106","西湖区","330100","DISTRICT");
        return rows;
    }
    private static void add(List<Change> rows,String table,String id,String name,String parent,String level) {
        ObjectNode n=Json.MAPPER.createObjectNode(); n.put("id",id); n.put("name",name);
        n.put("parent_id",parent); n.put("version",1L); n.put("update_time","2026-01-01 00:00:00");
        if(level!=null) n.put("region_level",level);
        rows.add(new Change(table,"c",n));
    }
}
