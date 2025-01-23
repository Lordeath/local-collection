package com.fxm.local.collection.test;

import com.fxm.local.collection.LocalList;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.fxm.local.collection.db.config.MainConfig.CONST_DB_ENGINE;
import static com.fxm.local.collection.test.SqliteTest.testCases;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class H2Test {

    @BeforeAll
    public static void before() {
        System.setProperty(CONST_DB_ENGINE, "h2");
        Thread.currentThread().setName("h2");
    }

    @AfterAll
    public static void after() {
        Thread.currentThread().setName("main");
    }

    /**
     * 测试新增和获取
     */
    @Test
    public void testInsertAndGet() throws Exception {
        testCases();
    }

}
