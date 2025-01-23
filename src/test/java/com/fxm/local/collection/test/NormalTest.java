package com.fxm.local.collection.test;

import com.fxm.local.collection.LocalList;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NormalTest {

    /**
     * 测试新增和获取
     * @throws Exception
     */
    @Test
    public void testInsertAndGet() throws Exception {
        try (LocalList<String> list = new LocalList<>(String.class);) {
            list.add("a");
            list.add("b");
            assertEquals(2, list.size());
            assertEquals("a", list.get(0));
            assertEquals("b", list.get(1));

            list.addAll(Lists.newArrayList("c", "d"));
            assertEquals(4, list.size());
            assertEquals("c", list.get(2));
            assertEquals("d", list.get(3));
        }
        try (LocalList<TestBean1> list = new LocalList<>(TestBean1.class);) {
            list.add(new TestBean1("Jack", 26));
            list.add(new TestBean1("Rose", 25));
            assertEquals("Jack", list.get(0).name);
            assertEquals(26, list.get(0).age);
            assertEquals("Rose", list.get(1).name);
            assertEquals(25, list.get(1).age);
            assertEquals(2, list.size());
        }
        try (LocalList<TestBean1> list = new LocalList<>(TestBean1.class);) {
            for (int i = 0; i < 10000; i++) {
                list.add(new TestBean1("Jack", i));
            }
            assertEquals(0, list.get(0).age);
            assertEquals(9999, list.get(9999).age);
            assertEquals(10000, list.size());
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBean1 {
        private String name;
        private int age;
    }
}
