package lordeath.local.collection.db.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 主配置类，用于配置数据库引擎类型
 */
@Getter
@RequiredArgsConstructor
public enum MainConfig {
    /** 数据库引擎类型配置键 */
    DB_ENGINE("lordeath.local.collection.db.engine", "sqlite"),
    CACHE_SIZE("lordeath.local.collection.cache.size", 1000 + ""),
    ;

    private final String key;
    private final String defaultValue;

    public String getProperty() {
        return System.getProperty(key, defaultValue);
    }

    public int getPropertyInt() {
        return Integer.parseInt(System.getProperty(key, defaultValue));
    }

    public String setProperty(String value) {
        return System.setProperty(key, value);
    }
}
