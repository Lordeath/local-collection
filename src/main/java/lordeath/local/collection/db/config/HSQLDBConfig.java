package lordeath.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.time.LocalDate;

public class HSQLDBConfig {
    // 用于配置HSQLDB的文件路径，注意，这个目录不可以和其他微服务共享，否则会出现数据不一致的问题
    public static final String CONST_HSQLDB_FILE_PATH = "lordeath.local.collection.hsqldb.file.path";
    public static final String DEFAULT_HSQLDB_FILE_PATH = "./local_collection/hsqldb/fxm_local_collection_";
    public static final String CONST_HSQLDB_USERNAME = "lordeath.local.collection.hsqldb.file.username";
    public static final String CONST_HSQLDB_PASSWORD = "lordeath.local.collection.hsqldb.file.password";

    private static DataSource dataSource;

    public static DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        init();
        return dataSource;
    }

    private static synchronized void init() {
        String filePath = System.getProperty(CONST_HSQLDB_FILE_PATH);
        if (StringUtils.isBlank(filePath)) {
            // 再加上年月日，用于自动清理过期的文件
            // 使用年月日时分秒的格式
            String date = LocalDate.now().toString();
            date = StringUtils.replace(date, ":", "_");
            date = StringUtils.replace(date, "-", "_");
            filePath = DEFAULT_HSQLDB_FILE_PATH + date;
        }
        String username = System.getProperty(CONST_HSQLDB_USERNAME);
        String password = System.getProperty(CONST_HSQLDB_PASSWORD);
        HikariDataSource hikariDataSource = new HikariDataSource();
//        hikariDataSource.setJdbcUrl("jdbc:hsqldb:file:" + filePath + ";DB_CLOSE_DELAY=-1;MODE=MySQL;");
        hikariDataSource.setJdbcUrl("jdbc:hsqldb:file:" + filePath);
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        dataSource = hikariDataSource;
    }

}
