package lordeath.local.collection.db.util;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * SQL 方言兼容工具
 */
public final class SqlDialectUtil {

    private static final String ENGINE_SQLITE = "sqlite";
    private static final String ENGINE_H2 = "h2";

    private SqlDialectUtil() {
    }

    /**
     * 构建 LocalMap 聚合场景下的 key 表达式。
     *
     * @param groupByColumns 分组字段
     * @param keyColumn      key列名
     * @param databaseEngine 数据库引擎
     * @return 表达式字符串
     */
    public static String buildGroupByKeyExpression(List<String> groupByColumns, String keyColumn, String databaseEngine) {
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            return "'' AS " + keyColumn;
        }
        String engine = StringUtils.isBlank(databaseEngine) ? ENGINE_SQLITE : databaseEngine;
        if (ENGINE_H2.equalsIgnoreCase(engine)) {
            return buildH2GroupKeyExpression(groupByColumns, keyColumn);
        }
        return buildSqliteGroupKeyExpression(groupByColumns, keyColumn);
    }

    private static String buildSqliteGroupKeyExpression(List<String> groupByColumns, String keyColumn) {
        return String.join(" || '.' || ", groupByColumns) + " AS " + keyColumn;
    }

    private static String buildH2GroupKeyExpression(List<String> groupByColumns, String keyColumn) {
        if (groupByColumns.size() == 1) {
            return groupByColumns.get(0) + " AS " + keyColumn;
        }
        StringBuilder expressionBuilder = new StringBuilder("CONCAT(");
        expressionBuilder.append(groupByColumns.get(0));
        for (int i = 1; i < groupByColumns.size(); i++) {
            expressionBuilder.append(", '.', ").append(groupByColumns.get(i));
        }
        expressionBuilder.append(")");
        return expressionBuilder + " AS " + keyColumn;
    }
}
