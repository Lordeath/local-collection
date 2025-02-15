package lordeath.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.File;

/**
 * H2数据库配置类
 */
public final class H2Config {
    /**
     * H2数据库文件路径配置键
     */
    public static final String CONST_H2_FILE_PATH = "lordeath.local.collection.h2.file.path";
    /**
     * H2数据库默认文件路径
     */
    public static final String DEFAULT_H2_FILE_PATH = "./local_collection/h2/fxm_local_collection";
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
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static synchronized void init() {
        String filePath = System.getProperty(CONST_H2_FILE_PATH);
        if (StringUtils.isBlank(filePath)) {
            filePath = DEFAULT_H2_FILE_PATH;

            // 判断是否存在 app 名称
            String appName = MainConfig.DB_ENGINE_APP_NAME.getProperty();
            // 原有的路径上，增加一级
            File fileWithAppName = new File(new File(filePath).getParent(), appName);
            filePath = new File(fileWithAppName, new File(filePath).getName()).getAbsolutePath();
        }
        if (MainConfig.DB_ENGINE_INIT_DELETE.getPropertyBoolean()) {
            // 启动时删除文件
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
        }

        String username = System.getProperty(CONST_H2_USERNAME);
        String password = System.getProperty(CONST_H2_PASSWORD);
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl("jdbc:h2:file:" + filePath + ";DB_CLOSE_DELAY=-1;MODE=MySQL;");
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        dataSource = hikariDataSource;
        // File file = new File(filePath);
        // file.deleteOnExit();
    }

}
