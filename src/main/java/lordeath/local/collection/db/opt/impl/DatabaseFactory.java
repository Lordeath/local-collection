package lordeath.local.collection.db.opt.impl;

import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.db.bean.LocalColumnForMap;
import lordeath.local.collection.db.config.MainConfig;
import lordeath.local.collection.db.opt.inter.IDatabaseOpt;

import java.util.List;


/**
 * 数据库操作对象工厂
 */
@Slf4j
public class DatabaseFactory {

    /**
     * 工具类，不允许实例化
     */
    private DatabaseFactory() {
    }

    /**
     * 创建数据库操作对象
     *
     * @param clazz 元素类型
     * @param <T>   元素类型
     * @return 数据库操作对象
     */
    public static <T> IDatabaseOpt<T> createDatabaseOptForList(Class<T> clazz) {
        // 这里可以根据配置或者其他条件选择不同的数据库实现
        // 目前默认使用H2数据库
        String dbEngine = MainConfig.DB_ENGINE.getProperty();
        IDatabaseOpt<T> databaseOpt;
        if (dbEngine == null || "sqlite".equalsIgnoreCase(dbEngine)) {
            // 默认的改成sqlite，因为sqlite的内存降低是最明显的
            databaseOpt = new SqliteOpt<>(clazz);
        } else if ("h2".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new H2Opt<>(clazz);
        } else {
            throw new IllegalArgumentException("其他的数据库暂时不支持: " + dbEngine);
        }
        return databaseOpt;
    }

    /**
     * 创建数据库操作对象 （map使用的内部list）
     * @param clazz 元素类型
     * @param tableName 表名
     * @param columnsForMap 列映射定义
     * @return 数据库操作对象
     * @param <T> 元素类型
     */
    public static <T> IDatabaseOpt<T> createDatabaseOptForMap(Class<T> clazz, String tableName, List<LocalColumnForMap> columnsForMap) {
        // 这里可以根据配置或者其他条件选择不同的数据库实现
        // 目前默认使用H2数据库
        String dbEngine = MainConfig.DB_ENGINE.getProperty();
        IDatabaseOpt<T> databaseOpt;
        if (dbEngine == null || "h2".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new H2Opt<>(clazz, tableName, columnsForMap);
        } else if ("sqlite".equalsIgnoreCase(dbEngine)) {
            databaseOpt = new SqliteOpt<>(clazz, tableName, columnsForMap);
        } else {
            throw new IllegalArgumentException("其他的数据库暂时不支持: " + dbEngine);
        }
        return databaseOpt;
    }
}
