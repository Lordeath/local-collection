package com.fxm.local.collection;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.util.ColumnNameUtil;
import com.fxm.local.collection.db.util.DBUtil;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 一个基于数据库的Map实现，继承自LocalList
 * 通过在数据库中创建索引来支持快速查找
 *
 * @param <K> Map的键类型，通常是String
 * @param <V> Map的值类型，必须是一个JavaBean类
 */
@Slf4j
public class LocalMap<K, V> implements Map<K, V>, AutoCloseable {
    private final String keyColumn;
    private final Class<V> valueClass;
    private final LocalList<V> innerList;

    private LocalMap(Class<V> valueClass, String keyColumn, LocalList<V> innerList) {
        this.valueClass = valueClass;
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
        return innerList.contains(value);
    }

    @Override
    public V get(Object key) {
        return (V) innerList.getDatabaseOpt().getByKey(keyColumn, key);
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("LocalMap is read-only");
    }

    @Override
    public V remove(Object key) {
        V value = get(key);
        if (value != null) {
            innerList.getDatabaseOpt().removeByKey(keyColumn, key);
        }
        return value;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("LocalMap is read-only");
    }

    @Override
    public void clear() {
        innerList.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        List<String> keys = innerList.getDatabaseOpt().getAllKeys(keyColumn);
        return new HashSet<>((Collection<K>) keys);
    }

    @Override
    public Collection<V> values() {
        return Collections.unmodifiableList(innerList);
    }

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

    public static <T> MapBuilder<T> from(LocalList<T> source) {
        return new MapBuilder<>(source);
    }

    public static class MapBuilder<T> {
        private final LocalList<T> source;
        private String whereClause;
        private final List<String> groupByColumns;
        private Class<?> resultClass;
        private Field keyField;

        private MapBuilder(LocalList<T> source) {
            this.source = source;
            this.groupByColumns = new ArrayList<>();
        }

        public MapBuilder<T> where(String whereClause) {
            this.whereClause = whereClause;
            return this;
        }

        public MapBuilder<T> groupBy(String... columns) {
            groupByColumns.addAll(Arrays.asList(columns));
            return this;
        }

        public MapBuilder<T> select(Class<?> resultClass) {
            this.resultClass = resultClass;
            return this;
        }

        public MapBuilder<T> keyField(Field keyField) {
            this.keyField = keyField;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <K, V> LocalMap<K, V> build() {
            // 生成新表名和key列名
            String newTableName = "map_" + UUID.randomUUID().toString().replace("-", "");
            String keyColumn = "key_" + UUID.randomUUID().toString().replace("-", "");

            // 获取结果类的字段信息
            List<LocalColumn> resultColumns = new ArrayList<>();

            // 添加key列
            Field keyField;
            try {
//                keyField = resultClass.getDeclaredMethod("getKey").getReturnType().getDeclaredField("value");
                keyField = this.keyField;
                keyField.setAccessible(true);
                resultColumns.add(new LocalColumn(
                        keyColumn,                    // columnName
                        keyField.getType(),          // columnType
                        DBUtil.getSqlType(keyField.getType()), // dbType
                        keyField                     // field
                ));
            } catch (Exception e) {
                throw new RuntimeException("Result class must have a getKey() method", e);
            }

            // 添加其他列
            resultColumns.addAll(ColumnNameUtil.getFields(resultClass));

//            // 创建新表
//            source.getDatabaseOpt().createGroupedTable(newTableName, groupByColumns, whereClause, keyColumn, resultColumns);
//
//            // 插入数据
//            source.getDatabaseOpt().insertGroupedData(source.getDatabaseOpt().getTableName(), newTableName,
//                    groupByColumns, whereClause, keyColumn, resultColumns);

            // 创建新的LocalList实例，使用自定义的列定义
            LocalList<V> innerList = new LocalList<>((Class<V>) resultClass, newTableName, resultColumns);

            // 创建新的LocalMap实例
            return new LocalMap<>((Class<V>) resultClass, keyColumn, innerList);
        }
    }
}
