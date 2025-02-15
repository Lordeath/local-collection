package lordeath.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;

/**
 * SQLite数据库配置类
 */
public class SqliteConfig {
    /**
     * SQLite数据库文件路径配置键
     */
    public static final String CONST_SQLITE_FILE_PATH = "lordeath.local.collection.sqlite.file.path";
    /**
     * SQLite数据库默认文件路径
     */
    public static final String DEFAULT_SQLITE_FILE_PATH = "./local_collection/sqlite/fxm_local_collection_";
    /**
     * SQLite数据库用户名配置键
     */
    public static final String CONST_SQLITE_USERNAME = "lordeath.local.collection.sqlite.file.username";
    /**
     * SQLite数据库密码配置键
     */
    public static final String CONST_SQLITE_PASSWORD = "lordeath.local.collection.sqlite.file.password";

    private static DataSource dataSource;

    /**
     * 获取SQLite数据库数据源
     * @return SQLite数据库数据源
     */
    public static DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        init();
        return dataSource;
    }

    /**
     * 初始化SQLite数据库数据源
     */
    private static synchronized void init() {
        String filePath = System.getProperty(CONST_SQLITE_FILE_PATH);
        if (StringUtils.isBlank(filePath)) {
            // 再加上年月日，用于自动清理过期的文件
            // 使用年月日时分秒的格式
            String date = LocalDate.now().toString();
            date = StringUtils.replace(date, ":", "_");
            date = StringUtils.replace(date, "-", "_");
            filePath = DEFAULT_SQLITE_FILE_PATH + date + ".sqlite";
        }
        try {
            FileUtils.forceMkdirParent(new File(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (MainConfig.DB_ENGINE_INIT_DELETE.getPropertyBoolean()) {
            // 启动时删除文件
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
        }

        String username = System.getProperty(CONST_SQLITE_USERNAME);
        String password = System.getProperty(CONST_SQLITE_PASSWORD);
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl("jdbc:sqlite:" + filePath);
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        dataSource = hikariDataSource;
        File file = new File(filePath);
        file.deleteOnExit();
    }

}
