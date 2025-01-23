package com.fxm.local.collection.db.factory;

import com.fxm.local.collection.db.bean.LocalColumn;
import com.fxm.local.collection.db.impl.DerbyOpt;
import com.fxm.local.collection.db.impl.H2Opt;
import com.fxm.local.collection.db.impl.HSQLDBOpt;
import com.fxm.local.collection.db.impl.SqliteOpt;
import com.fxm.local.collection.db.inter.IDatabaseOpt;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.fxm.local.collection.db.config.MainConfig.CONST_DB_ENGINE;

@Slf4j
public class DatabaseFactory {
    
    /**
     * 创建数据库操作对象
     * @param clazz 元素类型
     * @param tableName 表名
     * @param columns 列定义
     * @return 数据库操作对象
     * @param <T> 元素类型
     */
    public static <T> IDatabaseOpt<T> createDatabaseOpt(Class<T> clazz, String tableName, List<LocalColumn> columns) {
        // 这里可以根据配置或者其他条件选择不同的数据库实现
        // 目前默认使用H2数据库
        String dbEngine = System.getProperty(CONST_DB_ENGINE);
        IDatabaseOpt<T> databaseOpt;
        if (dbEngine == null || "h2".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new H2Opt<>(clazz, tableName, columns);
        } else if ("sqlite".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new SqliteOpt<>(clazz, tableName, columns);
        } else if ("hsqldb".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new HSQLDBOpt<>(clazz, tableName, columns);
        } else if ("derby".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new DerbyOpt<>(clazz, tableName, columns);
        } else {
            throw new IllegalArgumentException("其他的数据库暂时不支持: " + dbEngine);
        }
        return databaseOpt;
    }
}
