package com.fxm.local.collection.db.config;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class H2Config {

    public static final String CONST_H2_FILE_PATH = "com.fxm.local.collection.h2.file.path";
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
        String username = System.getProperty(CONST_H2_USERNAME);
        String password = System.getProperty(CONST_H2_PASSWORD);
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl("jdbc:h2:file:" + filePath + ";DB_CLOSE_DELAY=-1;MODE=MySQL;");
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        dataSource = hikariDataSource;
    }

}
