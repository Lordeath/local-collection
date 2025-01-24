package lordeath.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;

/**
 * Derby数据库配置类
 */
public class DerbyConfig {
    /**
     * Derby数据库文件路径配置键
     */
    public static final String CONST_DERBY_FILE_PATH = "lordeath.local.collection.derby.file.path";
    /**
     * Derby数据库默认文件路径
     */
    public static final String DEFAULT_DERBY_FILE_PATH = "./local_collection/derby/fxm_local_collection_";
    /**
     * Derby数据库用户名配置键
     */
    public static final String CONST_DERBY_USERNAME = "lordeath.local.collection.derby.file.username";
    /**
     * Derby数据库密码配置键
     */
    public static final String CONST_DERBY_PASSWORD = "lordeath.local.collection.derby.file.password";

    private static DataSource dataSource;

    /**
     * 获取Derby数据库数据源
     * @return Derby数据库数据源
     */
    public static DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        init();
        return dataSource;
    }

    /**
     * 初始化Derby数据库数据源
     */
    private static synchronized void init() {
        String filePath = System.getProperty(CONST_DERBY_FILE_PATH);
        if (StringUtils.isBlank(filePath)) {
            // 再加上年月日，用于自动清理过期的文件
            // 使用年月日时分秒的格式
            String date = LocalDate.now().toString();
            date = StringUtils.replace(date, ":", "_");
            date = StringUtils.replace(date, "-", "_");
            filePath = DEFAULT_DERBY_FILE_PATH + date;
        }
        try {
            FileUtils.forceMkdir(new File(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String username = System.getProperty(CONST_DERBY_USERNAME);
        String password = System.getProperty(CONST_DERBY_PASSWORD);
        HikariDataSource hikariDataSource = new HikariDataSource();
//        hikariDataSource.setJdbcUrl("jdbc:derby:file:" + filePath + ";DB_CLOSE_DELAY=-1;MODE=MySQL;");
        hikariDataSource.setJdbcUrl("jdbc:derby:" + filePath + ";create=true");
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        dataSource = hikariDataSource;
    }

}
