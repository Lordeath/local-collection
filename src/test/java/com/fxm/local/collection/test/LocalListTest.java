package com.fxm.local.collection.test;

import com.fxm.local.collection.LocalList;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class LocalListTest {
    public static void testCases() throws Exception {
        try (LocalList<String> list = new LocalList<>()) {
            list.add("a");
            list.add("b");
            assertEquals(2, list.size());
            assertEquals("a", list.get(0));
            assertEquals("b", list.get(1));   }
        try (LocalList<TestBean1> list = new LocalList<>()) {
            list.add(new TestBean1("Jack", 26));
            list.add(new TestBean1("Rose", 25));
            assertEquals(2, list.size());
            assertEquals("Jack", list.get(0).name);
            assertEquals(26, list.get(0).age);
            assertEquals("Rose", list.get(1).name);
            assertEquals(25, list.get(1).age);
        }

        try (LocalList<String> list = new LocalList<>(String.class)) {
            list.add("a");
            list.add("b");
            assertEquals(2, list.size());
            assertEquals("a", list.get(0));
            assertEquals("b", list.get(1));
            for (String s : list) {
                System.out.println(s);
            }


            assertEquals(1, list.subListInMemory(0, 1).size());
            assertEquals(2, list.subListInMemory(0, 2).size());
            list.add("c");
            list.add("d");
            assertEquals(1, list.subListInMemory(1, 2).size());
            list.remove(2);
            list.remove(2);

            list.set(1, "bb");
            assertEquals("bb", list.get(1));


            list.addAll(Lists.newArrayList("c", "d"));
            assertEquals(4, list.size());
            assertEquals("c", list.get(2));
            assertEquals("d", list.get(3));

            list.clear();
            assertEquals(0, list.size());
            assertTrue(list.isEmpty());

            list.add("a");
            list.add("b");
            assertEquals(2, list.size());

            list.remove(0);
            assertEquals(1, list.size());
            assertEquals("b", list.get(0));
            list.remove(0);
            assertEquals(0, list.size());
            list.add("a");
            list.add("b");
            assertEquals(2, list.size());
            list.remove(1);
            assertEquals(1, list.size());
            assertEquals("a", list.get(0));
        }
        try (LocalList<TestBean1> list = new LocalList<>(TestBean1.class)) {
            list.add(new TestBean1("Jack", 26));
            list.add(new TestBean1("Rose", 25));
            assertEquals(2, list.size());
            assertEquals("Jack", list.get(0).name);
            assertEquals(26, list.get(0).age);
            assertEquals("Rose", list.get(1).name);
            assertEquals(25, list.get(1).age);

            TestBean1 bean1 = list.get(1);
            // TODO 要注意，这里set之后不会影响到list中的数据，因为我们没有操作h2里面的数据
            bean1.setAge(99);
            list.set(1, bean1);
            assertEquals(99, list.get(1).age);

            list.remove(0);
            assertEquals(1, list.size());
            assertEquals("Rose", list.get(0).name);

            list.clear();
            assertEquals(0, list.size());
            list.add(new TestBean1("Jack", 26));
            list.add(new TestBean1("Rose", 25));
            assertEquals(2, list.size());
        }
        try (LocalList<TestBean1> list = new LocalList<>(TestBean1.class)) {
            for (int i = 0; i < 10000; i++) {
                list.add(new TestBean1("Jack", i));
            }
            assertEquals(0, list.get(0).age);
            assertEquals(9999, list.get(9999).age);
            assertEquals(10000, list.size());

            for (TestBean1 testBean1 : list) {
                log.info("正在遍历: {}", testBean1);
            }
            assertEquals(10000, list.subListInMemory(0, 10000).size());
            for (TestBean1 testBean1 : list.subListInMemory(0, 10000)) {
                log.info("正在内存遍历: {}", testBean1);
            }
            assertEquals(9999, list.subListInMemory(1, 10000).size());
            assertEquals(1, list.subListInMemory(1, 10000).get(0).age);

            list.clear();
            assertEquals(0, list.size());
            list.add(new TestBean1("Jack", 0));
            assertEquals(1, list.size());
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
