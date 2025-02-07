package lordeath.local.collection;

import lombok.Getter;
import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.util.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.util.*;

/**
 * 本地Map实现，基于数据库存储
 * @param <K> 键类型
 * @param <V> 值类型
 */
@Slf4j
@Getter
public class LocalMap<K extends String, V> implements Map<K, V>, AutoCloseable {
    private final String keyColumn;
    private final LocalList<V> innerList;

    /**
     * 构造一个新的LocalMap实例。
     * 
     * 该构造函数初始化一个带有随机生成键列的新LocalMap实例。
     * 它还创建一个新的LocalList实例来存储映射的值。
     */
    public LocalMap() {
        this.keyColumn = "key_" + UUID.randomUUID().toString().replace("-", "");
        innerList = new LocalList<>();
    }

    private LocalMap(String keyColumn, LocalList<V> innerList) {
        this.keyColumn = keyColumn;
        this.innerList = innerList;
    }

    @Override
    public int size() {
        return innerList.size();
    }

    @Override
    public boolean isEmpty() {
        return innerList.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return innerList.getDatabaseOpt().getByKey(keyColumn, key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        return innerList.getDatabaseOpt().getByKey(keyColumn, key);
    }

    @Override
    public V put(K key, V value) {
        if (innerList.getDatabaseOpt() == null) {
//            innerList.init((Class<V>) value.getClass());
            Class<?> resultClass = value.getClass();
            String newTableName = "map_" + UUID.randomUUID().toString().replace("-", "");

            List<LocalColumnForMap> columnForMapList = new ArrayList<>();

            {
                LocalColumnForMap localColumnForMap = new LocalColumnForMap();
                localColumnForMap.setKey(true);
//                localColumnForMap.setExpression(String.join(" || '.' || ", groupByColumns) + " AS " + keyColumn);
                localColumnForMap.setExpression(keyColumn);
                localColumnForMap.setSinkColumn(new LocalColumn(keyColumn, key.getClass(), DBUtil.getSqlType(key.getClass()), null));
                columnForMapList.add(localColumnForMap);
            }

            String sqlType = DBUtil.getSqlTypeOrNull(resultClass);
            if (sqlType != null) {
                LocalColumnForMap localColumnForMap = new LocalColumnForMap();
                localColumnForMap.setExpression("value_column");
                localColumnForMap.setSinkColumn(new LocalColumn("value_column", resultClass, sqlType, null));
                columnForMapList.add(localColumnForMap);
            } else {
                // 我们只需要关注我们自己需要采集的列就好，不需要把原先的列都采集过来
                for (Field fieldGot : FieldUtils.getAllFieldsList(resultClass)) {
                    String selectColumn = fieldGot.getName();
                    LocalColumnForMap localColumnForMap = new LocalColumnForMap();
                    localColumnForMap.setExpression(selectColumn);
                    String sinkColumnName = selectColumn;
                    sinkColumnName = StringUtils.replace(sinkColumnName, "\n", " ");
                    sinkColumnName = StringUtils.replace(sinkColumnName, "\r", " ");
                    if (StringUtils.containsIgnoreCase(sinkColumnName, " AS ")) {
                        // 有别名，拿到别名
                        sinkColumnName = selectColumn.substring(StringUtils.lastIndexOfIgnoreCase(selectColumn, " AS ") + " AS ".length()).trim();
                    }
                    // 通过别名，拿到字段名称
                    Field field = FieldUtils.getDeclaredField(resultClass, sinkColumnName, true);
                    if (field == null) {
                        throw new UnsupportedOperationException("目标的类无法找到对应的字段: " + selectColumn);
                    }
                    localColumnForMap.setSinkColumn(new LocalColumn(sinkColumnName, field.getType(), DBUtil.getSqlType(field.getType()), field));
                    columnForMapList.add(localColumnForMap);
                }
            }


            LocalList<V> innerList = new LocalList<>((Class<V>) value.getClass(), newTableName, columnForMapList);
            this.innerList.databaseOpt = innerList.databaseOpt;
            this.innerList.columns = innerList.columns;
        }
        return innerList.putByKey(keyColumn, key + "", value);
    }

    @Override
    public V remove(Object key) {
        V value = get(key);
        if (value != null) {
            innerList.removeByKey(keyColumn, key);
        }
        return value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        innerList.clear();
    }

    @Override
    @SuppressWarnings({"unchecked", "NullableProblems"})
    public Set<K> keySet() {
        List<String> keys = innerList.getDatabaseOpt().getAllKeys(keyColumn);
        return new HashSet<>((Collection<K>) keys);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Collection<V> values() {
        return Collections.unmodifiableList(innerList);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> entries = new HashSet<>();
        for (K key : keySet()) {
            V value = get(key);
            if (value != null) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            }
        }
        return Collections.unmodifiableSet(entries);
    }

    @Override
    public void close() {
        innerList.close();
    }

    /**
     * 创建MapBuilder实例
     * @param source 源数据列表
     * @param <T> 数据类型
     * @return MapBuilder实例
     */
    public static <T> MapBuilder<T> from(LocalList<T> source) {
        return new MapBuilder<>(source);
    }

    /**
     * Map构建器类
     * @param <T> 数据类型
     */
    public static class MapBuilder<T> {
        private final LocalList<T> source;
        private String whereClause;
        private final List<String> groupByColumns;
        private final List<String> selectColumns;
        private Class<?> resultClass;
        private Field keyField;

        private MapBuilder(LocalList<T> source) {
            this.source = source;
            this.groupByColumns = new ArrayList<>();
            selectColumns = new ArrayList<>();
        }

        /**
         * 设置分组字段
         * @param columns 分组字段列表
         * @return 当前构建器实例
         */
        public MapBuilder<T> groupBy(String... columns) {
            groupByColumns.addAll(Arrays.asList(columns));
            return this;
        }

        /**
         * 设置过滤条件
         * @param whereClause 过滤条件
         * @return 当前构建器实例
         */
        public MapBuilder<T> where(String whereClause) {
            this.whereClause = whereClause;
            return this;
        }

        /**
         * 设置选择字段
         * @param columns 选择字段列表
         * @return 当前构建器实例
         */
        public MapBuilder<T> select(String... columns) {
            selectColumns.addAll(Arrays.asList(columns));
            return this;
        }

        /**
         * 设置结果类型
         * @param resultClass 结果类型
         * @return 当前构建器实例
         */
        public MapBuilder<T> resultClass(Class<?> resultClass) {
            this.resultClass = resultClass;
            return this;
        }

        /**
         * 设置键字段
         * @param keyField 键字段
         * @return 当前构建器实例
         */
        public MapBuilder<T> keyField(Field keyField) {
            this.keyField = keyField;
            return this;
        }

        /**
         * 构建LocalMap实例
         * @param <K> 键类型
         * @param <V> 值类型
         * @return LocalMap实例
         */
        @SuppressWarnings("unchecked")
        public <K extends String, V> LocalMap<K, V> build() {
            // 生成新表名和key列名
            String newTableName = "map_" + UUID.randomUUID().toString().replace("-", "");
            String keyColumn = "key_" + UUID.randomUUID().toString().replace("-", "");

            List<LocalColumnForMap> columnForMapList = getLocalColumnForMaps(keyColumn);

            // 创建新的LocalList实例，使用自定义的列定义
            LocalList<V> innerList = new LocalList<>((Class<V>) resultClass, newTableName, columnForMapList);

            // 给这个innerList灌入数据，insert into sink_table (...) select ... from source_table where ... group by ...
            innerList.insertGroupedData(source.getDatabaseOpt().getTableName()
                    , newTableName, groupByColumns, whereClause, columnForMapList);

            // 创建新的LocalMap实例
            return new LocalMap<>(keyColumn, innerList);
        }

        private List<LocalColumnForMap> getLocalColumnForMaps(String keyColumn) {
            List<LocalColumnForMap> columnForMapList = new ArrayList<>();

            {
                LocalColumnForMap localColumnForMap = new LocalColumnForMap();
                localColumnForMap.setKey(true);
                // TODO 这个表达式可能不是都兼容的，如果都兼容就不用改成接口各自实现，如果不兼容就要实现一个获取字段拼接的表达式的接口
                localColumnForMap.setExpression(String.join(" || '.' || ", groupByColumns) + " AS " + keyColumn);
                localColumnForMap.setSinkColumn(new LocalColumn(keyColumn, keyField.getType(), DBUtil.getSqlType(keyField.getType()), keyField));
                columnForMapList.add(localColumnForMap);
            }

            // 我们只需要关注我们自己需要采集的列就好，不需要把原先的列都采集过来
            for (String selectColumn : selectColumns) {
                LocalColumnForMap localColumnForMap = new LocalColumnForMap();
                localColumnForMap.setExpression(selectColumn);
                String sinkColumnName = selectColumn;
                sinkColumnName = StringUtils.replace(sinkColumnName, "\n", " ");
                sinkColumnName = StringUtils.replace(sinkColumnName, "\r", " ");
                if (StringUtils.containsIgnoreCase(sinkColumnName, " AS ")) {
                    // 有别名，拿到别名
                    sinkColumnName = selectColumn.substring(StringUtils.lastIndexOfIgnoreCase(selectColumn, " AS ") + " AS ".length()).trim();
                }
                // 通过别名，拿到字段名称
                Field field = FieldUtils.getDeclaredField(resultClass, sinkColumnName, true);
                if (field == null) {
                    throw new UnsupportedOperationException("目标的类无法找到对应的字段: " + selectColumn);
                }
                localColumnForMap.setSinkColumn(new LocalColumn(sinkColumnName, field.getType(), DBUtil.getSqlType(field.getType()), field));
                columnForMapList.add(localColumnForMap);
            }
            return columnForMapList;
        }

    }
}
