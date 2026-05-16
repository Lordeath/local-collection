package lordeath.local.collection.db.opt.impl;

import lordeath.local.collection.db.bean.LocalColumn;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.config.H2Config;
import lordeath.local.collection.db.config.MainConfig;
import lordeath.local.collection.db.opt.inter.IDatabaseOpt;
import lordeath.local.collection.db.util.ColumnNameUtil;
import lordeath.local.collection.db.util.DBUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * H2数据库操作实现类
 * 通过操作H2数据库来实现对数据的操作，注意，这个类是线程不安�
�的
 *
 * @param <T> 数据类型
 */
@Slf4j
class H2Opt<T> implements IDatabaseOpt<T> {

    /**
     * 数据源
     */
    @Getter
    private final DataSource dataSource;
    /**
     * 操作的表名
     */
    @Getter
    private final String tableName;
    /**
     * 主键列名
     */
    private final String pkColumnName;
    /**
     * 列定义
     */
    private final List<LocalColumn> columns;
    /**
     * �
�素类型
     */
    private final Class<T> clazz;

    /**
     * 使用指定的�
�素类型构造数据库操作对象
     *
     * @param clazz �
�素类型
     */
    H2Opt(Class<T> clazz) {
        this.clazz = clazz;
        dataSource = H2Config.getDataSource();
        tableName = "tmp_" + UUID.randomUUID().toString().replace("-", "");
        pkColumnName = "id" + UUID.randomUUID().toString().replace("-", "");
        log.debug("开始初始化数据源（用class）: {} {}", dataSource, tableName);
        columns = Collections.unmodifiableList(ColumnNameUtil.getFields(clazz));
        // 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (").append(pkColumnName).append(" BIGINT AUTO_INCREMENT PRIMARY KEY");
        for (LocalColumn column : columns) {
            sql.append(", ").append(column.getColumnName()).append(" ").append(column.getDbType());
        }
        sql.append(");");
        log.debug("创建表的sql（用class）: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        log.debug("数据源初始化完毕（用class）: {} {}", dataSource, tableName);
    }

    /**
     * 使用指定的�
�素类型、表名和列映射构造数据库操作对象
     *
     * @param clazz         �
�素类型
     * @param tableName     表名
     * @param columnsForMap 列映射定义
     */
    H2Opt(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.columns = columnsForMap.stream().map(LocalColumnForMap::getSinkColumn).collect(Collectors.toList());
        dataSource = H2Config.getDataSource();
        log.debug("开始初始化数据源（用于Map）: {} {}", dataSource, tableName);
        // 创建表
        // 1. 获取到表名
        // 2. 获取到列名和类型
        // 3. 创建表
        StringBuilder sql = new StringBuilder("create table ").append(tableName)
                .append(" (");
        for (LocalColumnForMap localColumnForMap : columnsForMap) {
            LocalColumn column = localColumnForMap.getSinkColumn();
            sql.append(column.getColumnName()).append(" ").append(column.getDbType()).append(", ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(");");
        log.debug("创建表的sql（用于Map）: {}", sql);
        // 执行sql
        DBUtil.executeSql(dataSource, sql.toString());
        // 使用 columnsForMap的isKey判断是否是
        String pks = columnsForMap.stream().filter(LocalColumnForMap::isKey).map(m -> m.getSinkColumn().getColumnName()).collect(Collectors.joining(","));
        if (MainConfig.DB_CREATE_INDEX.getPropertyBoolean()) {
            sql = new StringBuilder("create index idx_").append(StringUtils.replace(pks, ",", "_")).
                    append(" ON ").append(tableName).append("(").append(pks).append(")");
            log.debug("M-hM-!M-(M-eM-^HM-^[M-eM-;M-:M-eM-.M-^LM-fM-/M-^UM-oM-<M-^LM-fM-^NM-%M-dM-8M-^KM-fM-^]M-%M-hM-.M->M-gM-=M-.mapM-gM-^ZM-^DkeyM-gM-4M-"M-eM-<M-^U: {} {}", sql);
            DBUtil.executeSql(dataSource, sql.toString());
        }
        pkColumnName = null;
        log.debug("数据源初始化完毕（用于Map）: {} {}", dataSource, tableName);
    }

    /**
     * 添加�
�素到数据库
     *
     * @param obj �
�素
     * @return 添加成功与否
     */
    @Override
    public boolean add(T obj) {
        return DBUtil.add(obj, tableName, columns, dataSource);
    }

    /**
     * 批量添加�
�素到数据库
     *
     * @param c �
�素集合
     * @return 添加成功与否
     */
    @Override
    public boolean addAll(Collection<? extends T> c) {
        return DBUtil.addAll(c, tableName, columns, dataSource);
    }

    /**
     * 移除指定索引的�
�素
     *
     * @param index 索引
     * @return 移除的�
�素
     */
    @Override
    public T remove(int index) {
        return DBUtil.remove(index, tableName, pkColumnName, columns, dataSource, clazz);
    }

    /**
     * �
空数据库
     */
    @Override
    public void clear() {
        DBUtil.clear(tableName, dataSource);
    }

    /**
     * �
�闭数据库连接
     */
    @Override
    public void close() {
        DBUtil.drop(tableName, dataSource);
    }

    /**
     * 获取数据库大小
     *
     * @return 大小
     */
    @Override
    public int size() {
        return DBUtil.size(tableName, dataSource);
    }

    /**
     * 获取指定索引的�
�素
     *
     * @param index      索引
     * @param removeFlag 用于判断是否对集合操作过移除，如果没有操作过移除，那就可以用下标+1作为id来进行查询
     * @return �
�素
     */
    @Override
    public T get(int index, boolean removeFlag) {
        return DBUtil.get(index, tableName, columns, pkColumnName, dataSource, clazz, removeFlag);
    }

    /**
     * 设置指定索引的�
�素
     *
     * @param index   索引
     * @param element �
�素
     * @return 原�
�素
     */
    @Override
    public T set(int index, T element) {
        return DBUtil.set(index, element, tableName, columns, pkColumnName, dataSource);
    }

    /**
     * 获取指定索引的主键值
     *
     * @param index 索引
     * @return 主键值
     */
    @Override
    public long pk(int index) {
        return DBUtil.pk(index, tableName, pkColumnName, dataSource);
    }

    /**
     * 批量查询�
�素
     *
     * @param fromIndex 开始索引
     * @param toIndex   结束索引
     * @return �
�素集合
     */
    @Override
    public List<T> batchQuery(int fromIndex, int toIndex) {
        return DBUtil.batchQuery(fromIndex, toIndex, tableName, columns, pkColumnName, dataSource, clazz);
    }

    /**
     * 创建分组表
     *
     * @param newTableName   新表名
     * @param groupByColumns 分组列
     * @param whereClause    条件
     * @param keyColumn      主键列
     * @param resultColumns  结果列
     * @return 创建成功与否
     */
    @Override
    public boolean createGroupedTable(String newTableName, List<String> groupByColumns, String whereClause, String keyColumn, List<LocalColumn> resultColumns) {
        return DBUtil.createGroupedTable(dataSource, newTableName, keyColumn, resultColumns);
    }

    /**
     * 插�
�分组数据
     *
     * @param sourceTableName  源表名
     * @param targetTableName  目标表名
     * @param groupByColumns   分组列
     * @param whereClause      条件
     * @param columnForMapList 列映射定义
     * @return 插�
�成功与否
     */
    @Override
    public boolean insertGroupedData(String sourceTableName, String targetTableName, List<String> groupByColumns, String whereClause,
                                     List<LocalColumnForMap> columnForMapList) {
        return DBUtil.insertGroupedData(dataSource, sourceTableName, targetTableName, groupByColumns, whereClause, columnForMapList);
    }

    /**
     * 根据主键获取�
�素
     *
     * @param keyColumn 主键列
     * @param keyValue  主键值
     * @return �
�素
     */
    @Override
    public T getByKey(String keyColumn, Object keyValue) {
        return DBUtil.getByKey(dataSource, tableName, keyColumn, keyValue, columns, clazz);
    }

    /**
     * 根据主键设置�
�素
     *
     * @param keyColumn 主键列
     * @param key       主键值
     * @param value     �
�素
     * @param removed   是否被移除，值在方法里面更新
     * @return 原�
�素
     */
    @Override
    public T putByKey(String keyColumn, String key, T value, AtomicBoolean removed) {
        return DBUtil.putByKey(dataSource, tableName, keyColumn, key, value, columns, removed);
    }

    /**
     * 根据主键移除�
�素
     *
     * @param keyColumn 主键列
     * @param keyValue  主键值
     * @return 移除成功与否
     */
    @Override
    public boolean removeByKey(String keyColumn, Object keyValue) {
        return DBUtil.removeByKey(dataSource, tableName, keyColumn, keyValue);
    }

    /**
     * 获取所有主键值
     *
     * @param keyColumn 主键列
     * @return 主键值集合
     */
    @Override
    public List<String> getAllKeys(String keyColumn) {
        return DBUtil.getAllKeys(dataSource, tableName, keyColumn);
    }

    @Override
    public String getDatabaseEngine() {
        return "h2";
    }

}
