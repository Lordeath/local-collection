package lordeath.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.time.LocalDate;

/**
 * H2数据库配置类
 */
public class H2Config {
    /**
     * H2数据库文件路径配置键
     */
    public static final String CONST_H2_FILE_PATH = "lordeath.local.collection.h2.file.path";
    /**
     * H2数据库默认文件路径
     */
    public static final String DEFAULT_H2_FILE_PATH = "./local_collection/h2/fxm_local_collection_";
    /**
     * H2数据库用户名配置键
     */
    public static final String CONST_H2_USERNAME = "lordeath.local.collection.h2.file.username";
    /**
     * H2数据库密码配置键
     */
    public static final String CONST_H2_PASSWORD = "lordeath.local.collection.h2.file.password";

    private static DataSource dataSource;

    /**
     * 获取H2数据库数据源
     * @return H2数据库数据源
     */
    public static DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        init();
        return dataSource;
    }

    /**
     * 初始化H2数据库数据源
     */
    private static synchronized void init() {
        String filePath = System.getProperty(CONST_H2_FILE_PATH);
        if (StringUtils.isBlank(filePath)) {
            // 再加上年月日，用于自动清理过期的文件
            // 使用年月日时分秒的格式
            String date = LocalDate.now().toString();
            date = StringUtils.replace(date, ":", "_");
            date = StringUtils.replace(date, "-", "_");
            filePath = DEFAULT_H2_FILE_PATH + date;
        }
        String username = System.getProperty(CONST_H2_USERNAME);
        String password = System.getProperty(CONST_H2_PASSWORD);
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl("jdbc:h2:file:" + filePath + ";DB_CLOSE_DELAY=-1;MODE=MySQL;");
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        dataSource = hikariDataSource;
    }

}
