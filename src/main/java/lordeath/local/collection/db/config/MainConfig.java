package lordeath.local.collection.db.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * 主配置类，用于配置数据库引擎类型
 */
@Getter
@RequiredArgsConstructor
public enum MainConfig {
    /**
     * 数据库引擎类型配置键
     */
    DB_ENGINE("lordeath.local.collection.db.engine", "sqlite"),
    /**
     * 是否要在启动时删除原有的数据文件
     */
    DB_ENGINE_INIT_DELETE("lordeath.local.collection.db.init.delete", "true"),
    /**
     * 写入缓存的触发写入的阈值，默认为1000
     */
    CACHE_SIZE("lordeath.local.collection.cache.size", 1000 + ""),
    ;

    private final String key;
    private final String defaultValue;

    /**
     * 获取当前的配置，如果获取不到哦配置，就使用默认的值
     *
     * @return 配置的值
     */
    public String getProperty() {
        String property = System.getProperty(key);
        if (StringUtils.isNotBlank(property)) {
            return property;
        }
        property = System.getenv(key);
        if (StringUtils.isNotBlank(property)) {
            return property;
        }
        return defaultValue;
    }

    /**
     * 获取当前的配置，如果获取不到哦配置，就使用默认的值
     *
     * @return 配置的值(int)
     */
    public int getPropertyInt() {
        return Integer.parseInt(getProperty());
    }

    /**
     * 获取当前的配置，如果获取不到哦配置，就使用默认的值
     *
     * @return 配置的值(boolean)
     */
    public boolean getPropertyBoolean() {
        return Boolean.parseBoolean(getProperty());
    }

    /**
     * 配置key对应的值
     *
     * @param value 配置的值
     * @return 配置的值
     */
    @SuppressWarnings("UnusedReturnValue")
    public String setProperty(String value) {
        return System.setProperty(key, value);
    }
}
