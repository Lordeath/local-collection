package com.fxm.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.time.LocalDate;

public class H2Config {
    // 用于配置h2的文件路径，注意，这个目录不可以和其他微服务共享，否则会出现数据不一致的问题
    public static final String CONST_H2_FILE_PATH = "com.fxm.local.collection.h2.file.path";
    public static final String DEFAULT_H2_FILE_PATH = "./local_collection/h2/fxm_local_collection_";
    public static final String CONST_H2_USERNAME = "com.fxm.local.collection.h2.file.username";
    public static final String CONST_H2_PASSWORD = "com.fxm.local.collection.h2.file.password";

    private static DataSource dataSource;

    public static DataSource getDataSource() {
        if (dataSource != null) {
            return dataSource;
        }
        init();
        return dataSource;
    }

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
