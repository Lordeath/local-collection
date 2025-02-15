package lordeath.local.collection.test;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.LocalList;
import lordeath.local.collection.LocalMap;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class LocalListTest {
    public static void testCases() {
        testList();
        testMap();
    }

    @SuppressWarnings("ConstantValue")
    static void testList() {
        try (LocalList<TestBean2> list = new LocalList<>()) {
            list.add(new TestBean2("Jack", 26, new Date(10000000), new BigDecimal("123.456789")));
            list.add(new TestBean2("Rose", 27, new Date(20000000), new BigDecimal("789.654321")));
            list.add(new TestBean2("Max", 28, new Date(30000000), new BigDecimal("123456789.123456789")));
            assertEquals(3, list.size());
            assertEquals(new Date(10000000), list.get(0).getBirthTime());
            assertEquals(new BigDecimal("123.456789"), list.get(0).getMoney());
            assertEquals(new Date(20000000), list.get(1).getBirthTime());
            assertEquals(new BigDecimal("789.654321"), list.get(1).getMoney());
            assertEquals(new BigDecimal("123456789.123456789"), list.get(2).getMoney());
            assertEquals(1, list.pk(0));
            assertEquals(2, list.pk(1));
            assertEquals(3, list.pk(2));
            list.remove(1);
            assertEquals(1, list.pk(0));
            assertEquals(3, list.pk(1));
        }


        try (LocalList<String> list = new LocalList<>()) {
            list.add("a");
            list.add("b");
            assertEquals(2, list.size());
            assertEquals("a", list.get(0));
            assertEquals("b", list.get(1));
        }

        try (LocalList<String> list = new LocalList<>(String.class)) {
            list.add("a");
            list.add("b");
            assertEquals(2, list.size());
            assertEquals("a", list.get(0));
            assertEquals("b", list.get(1));

            assertEquals(1, list.subList(0, 1).size());
            assertEquals(2, list.subList(0, 2).size());
            list.add("c");
            list.add("d");
            assertEquals(1, list.subList(1, 2).size());
            list.remove(2);
            list.remove(2);

            list.set(1, "bb");
            assertEquals("a", list.get(0));
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
            for (int i = 0; i < 100; i++) {
                list.add(new TestBean1("Jack", i));
            }
            assertEquals(0, list.get(0).age);
            assertEquals(99, list.get(99).age);
            assertEquals(100, list.size());

            for (TestBean1 testBean1 : list) {
                log.debug("正在遍历: {}", testBean1);
            }
            assertEquals(100, list.subList(0, 100).size());
            for (TestBean1 testBean1 : list.subList(0, 100)) {
                log.debug("正在内存遍历: {}", testBean1);
            }
            assertEquals(99, list.subList(1, 100).size());
            assertEquals(1, list.subList(1, 100).get(0).age);

            list.clear();
            assertEquals(0, list.size());
            list.add(new TestBean1("Jack", 0));
            assertEquals(1, list.size());
        }
    }

    @SuppressWarnings("ConstantValue")
    public static void testMap() {

        try (LocalList<TestBean1> list = new LocalList<>()) {
            list.add(new TestBean1("Jack", 26));
            list.add(new TestBean1("Rose", 25));
            assertEquals(2, list.size());
            assertEquals("Jack", list.get(0).name);
            assertEquals(26, list.get(0).age);
            assertEquals("Rose", list.get(1).name);
            assertEquals(25, list.get(1).age);

            // 创建Map，key是userId，value是UserOrderStats对象
            try (LocalMap<String, TestBean1> map = LocalMap.from(list)
                    .where("age >= 26")
                    .groupBy("name")
                    .select("name", "sum(age) AS age")
                    .resultClass(TestBean1.class)  // 指定结果类型
                    .keyField(FieldUtils.getDeclaredField(TestBean1.class, "name", true))
                    .build()) {

                assertEquals(1, map.size());
                assertNotNull(map.get("Jack"));
                assertEquals("Jack", map.get("Jack").name);
                map.clear();
                assertEquals(0, map.size());
                map.put("Jack", new TestBean1("Jack", 26));
                assertEquals(1, map.size());


            }

        }

        try (LocalMap<String, String> map = new LocalMap<>()) {
            map.put("a", "b");

            assertEquals("a", map.keySet().iterator().next());
            assertEquals("b", map.get("a"));
            assertEquals("b", map.getInnerList().get(0));

            map.remove("a");
            assertEquals(0, map.size());
        }

        try (LocalMap<String, TestBean1> map = new LocalMap<>()) {
            map.put("a", new TestBean1("1", 2));
            assertEquals("a", map.keySet().iterator().next());
            assertEquals("1", map.get("a").name);
            assertEquals(2, map.get("a").age);

            map.remove("a");
            assertEquals(0, map.size());
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBean1 {
        private String name;
        private int age;
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestBean2 implements Serializable {
        private static final long serialVersionUID = 5005900671586173979L;
        private String name;
        private int age;
        private Date birthTime;
        private BigDecimal money;
    }
}
