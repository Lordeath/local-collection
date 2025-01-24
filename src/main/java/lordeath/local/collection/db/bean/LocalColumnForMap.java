package lordeath.local.collection.db.bean;

import lombok.Data;

@Data
public class LocalColumnForMap {
//    // 记录数据源的格式相关的东西
//    private LocalColumn sourceColumn;
    // 目标的对象的相关字段信息
    private LocalColumn sinkColumn;
    // 表达式，比如 avg(age) AS age
    private String expression;
    private boolean isKey = false;
}
