package lordeath.local.collection.test;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lordeath.local.collection.LocalList;
import lordeath.local.collection.LocalMap;
import lordeath.local.collection.SynchronizedLocalMap;
import lordeath.local.collection.serialize.TypeCodec;
import lordeath.local.collection.serialize.TypeCodecRegistry;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class LocalListTest {
    private static final String CACHE_SIZE_KEY = "lordeath.local.collection.cache.size";
    private static final String CACHE_FLUSH_INTERVAL_MILLIS_KEY = "lordeath.local.collection.cache.flush.interval.millis";
    private static final String CACHE_FLUSH_CHUNK_SIZE_KEY = "lordeath.local.collection.cache.flush.chunk.size";

    public static void testCases() {
        testList();
        testMap();
        testUnsupportedOperations();
        testAddAllBranches();
        testDbIteratorAndListIterator();
        testSubListDbValidation();
        testPkWithRemoveFlag();
        testRuntimeMetrics();
        testRuntimeMetricsWithDirectDatabaseWrites();
        testRuntimeMetricsWithFlushIntervalAndChunkConfig();
        testSnapshotImportExport();
        testPluggableSerialization();
        testRecoveryStateApi();
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
                log.debug("sublist iterating: {}", testBean1);
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

            try (LocalMap<String, TestBean1> map2 = LocalMap.from(list)
                    .where("age >= 25")
                    .groupBy("name", "age")
                    .select("name", "sum(age) AS age")
                    .resultClass(TestBean1.class)
                    .keyField(FieldUtils.getDeclaredField(TestBean1.class, "name", true))
                    .build()) {
                assertEquals(2, map2.size());
                TestBean1 jack = map2.get("Jack.26");
                assertNotNull(jack);
                assertEquals("Jack", jack.name);
                assertEquals(26, jack.age);
                TestBean1 rose = map2.get("Rose.25");
                assertNotNull(rose);
                assertEquals("Rose", rose.name);
                assertEquals(25, rose.age);
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

        try (LocalMap<String, String> map = new LocalMap<>()) {
            assertNull(map.put("a", "1"));
            assertEquals(1, map.size());
            assertEquals("1", map.get("a"));

            assertEquals("1", map.put("a", "2"));
            assertEquals(1, map.size());
            assertEquals("2", map.get("a"));

            assertEquals("2", map.remove("a"));
            assertEquals(0, map.size());

            assertDoesNotThrow(() -> invokeRemoveByKey(map.getInnerList(), map.getKeyColumn(), "a"));
            assertEquals(0, map.size());

            assertNull(map.putIfAbsent("b", "2"));
            assertEquals("2", map.get("b"));
            assertEquals("2", map.putIfAbsent("b", "3"));

            assertNull(map.computeIfAbsent("c", (k) -> "4"));
            assertEquals("4", map.get("c"));
            assertEquals("4", map.computeIfAbsent("c", (k) -> "5"));

            assertFalse(map.removeIfEquals("c", "5"));
            assertTrue(map.removeIfEquals("c", "4"));
            assertEquals(1, map.size());
            assertNull(map.get("c"));
            java.util.Map<String, String> batch = new java.util.HashMap<>();
            batch.put("b", "2");
            batch.put("c", "3");
            map.put("b", "1");
            map.putAllIfAbsent(batch);
            assertEquals("1", map.get("b"));
            assertEquals("3", map.get("c"));

            java.util.Map<String, String> batchExistingProbe = new java.util.HashMap<>();
            batchExistingProbe.put("b", "1");
            batchExistingProbe.put("c", "7");
            java.util.Map<String, String> batchExisting = map.putAllIfAbsentWithExisting(batchExistingProbe);
            assertEquals(2, batchExisting.size());
            assertEquals("1", batchExisting.get("b"));
            assertEquals("3", batchExisting.get("c"));
            assertTrue(batchExisting.containsKey("c"));
            assertEquals("1", map.get("b"));
            assertEquals("3", map.get("c"));
        }

        try (SynchronizedLocalMap<String, String> synchronizedMap = LocalMap.synchronizedMap(new LocalMap<>())) {
            assertNull(synchronizedMap.putIfAbsent("a", "1"));
            assertEquals("1", synchronizedMap.get("a"));
            assertEquals("1", synchronizedMap.putIfAbsent("a", "2"));
            assertEquals("1", synchronizedMap.computeIfAbsent("a", (k) -> "3"));
            assertFalse(synchronizedMap.removeIfEquals("a", "2"));
            assertTrue(synchronizedMap.removeIfEquals("a", "1"));
            assertEquals(0, synchronizedMap.size());
            assertNull(synchronizedMap.get("a"));
            java.util.Map<String, String> syncBatch = new java.util.HashMap<>();
            syncBatch.put("a", "2");
            syncBatch.put("b", "1");
            synchronizedMap.put("a", "1");
            synchronizedMap.putAllIfAbsent(syncBatch);
            assertEquals("1", synchronizedMap.get("a"));
            assertEquals("1", synchronizedMap.get("b"));

            java.util.Map<String, String> syncExistingProbe = new java.util.HashMap<>();
            syncExistingProbe.put("a", "x");
            syncExistingProbe.put("c", "y");
            java.util.Map<String, String> syncExisting = synchronizedMap.putAllIfAbsentWithExisting(syncExistingProbe);
            assertEquals(1, syncExisting.size());
            assertEquals("1", syncExisting.get("a"));
            assertEquals("1", synchronizedMap.get("a"));
            assertEquals("y", synchronizedMap.get("c"));

            java.util.Map<String, String> removed = synchronizedMap.removeIfEquals(syncExisting);
            assertEquals(1, removed.size());
            assertEquals("1", removed.get("a"));
            assertEquals(2, synchronizedMap.size());
            assertEquals("1", synchronizedMap.get("b"));
        }

    }

    private static void testUnsupportedOperations() {
        try (LocalList<String> list = new LocalList<>(String.class)) {
            assertThrows(UnsupportedOperationException.class, () -> list.contains("a"));
            assertThrows(UnsupportedOperationException.class, () -> list.toArray());
            assertThrows(UnsupportedOperationException.class, () -> list.toArray(new String[0]));
            assertThrows(UnsupportedOperationException.class, () -> list.remove((Object) "a"));
            assertThrows(UnsupportedOperationException.class, () -> list.containsAll(Collections.singleton("a")));
            assertThrows(UnsupportedOperationException.class, () -> list.addAll(0, Collections.singletonList("a")));
            assertThrows(UnsupportedOperationException.class, () -> list.removeAll(Collections.singleton("a")));
            assertThrows(UnsupportedOperationException.class, () -> list.retainAll(Collections.singleton("a")));
            assertThrows(UnsupportedOperationException.class, () -> list.add(0, "a"));
            assertThrows(UnsupportedOperationException.class, () -> list.indexOf("a"));
            assertThrows(UnsupportedOperationException.class, () -> list.lastIndexOf("a"));
        }
    }

    private static void testAddAllBranches() {
        withCacheSize(0, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                assertTrue(list.addAll(Lists.newArrayList("a", "b")));
                assertEquals(2, list.size());
                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
            }
        });

        withCacheSize(2, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.add("x");
                assertTrue(list.addAll(Lists.newArrayList("a", "b")));
                assertEquals(3, list.size());
                assertEquals("x", list.get(0));
                assertEquals("a", list.get(1));
                assertEquals("b", list.get(2));
            }
        });

        withCacheSize(2, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.add("a");
                list.add("b");
                assertTrue(list.addAll(Collections.singletonList("c")));
                assertEquals(3, list.size());
                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
                assertEquals("c", list.get(2));
            }
        });

        withCacheSize(3, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.add("a");
                list.add("b");
                assertTrue(list.addAll(Collections.singletonList("c")));
                assertEquals(3, list.size());
                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
                assertEquals("c", list.get(2));
            }
        });

        try (LocalList<String> list = new LocalList<>()) {
            RuntimeException ex = assertThrows(RuntimeException.class, () -> list.addAll(Collections.emptyList()));
            assertTrue(ex.getMessage().contains("数据源操作初始化失败"));
        }
    }

    private static void testDbIteratorAndListIterator() {
        withCacheSize(1, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.add("a");
                list.add("b");
                assertEquals(2, list.size());

                Iterator<String> it = list.iterator();
                assertTrue(it.hasNext());
                assertEquals("a", it.next());
                assertTrue(it.hasNext());
                assertEquals("b", it.next());
                assertFalse(it.hasNext());

                assertThrows(IndexOutOfBoundsException.class, () -> list.listIterator(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> list.listIterator(list.size() + 1));
                assertThrows(NoSuchElementException.class, () -> list.listIterator(list.size()).next());
                assertThrows(NoSuchElementException.class, () -> list.listIterator(0).previous());

                ListIterator<String> lit = list.listIterator();
                assertThrows(IllegalStateException.class, lit::remove);
                assertThrows(IllegalStateException.class, () -> lit.set("x"));

                assertEquals("a", lit.next());
                assertTrue(lit.hasPrevious());
                assertEquals("a", lit.previous());

                lit.next();
                list.clear();
                assertThrows(ConcurrentModificationException.class, lit::remove);
            }
        });
    }

    private static void testSubListDbValidation() {
        withCacheSize(0, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.addAll(Lists.newArrayList("a", "b", "c"));

                assertThrows(IndexOutOfBoundsException.class, () -> list.subList(-1, 0));
                assertThrows(IndexOutOfBoundsException.class, () -> list.subList(0, list.size() + 1));
                assertThrows(IllegalArgumentException.class, () -> list.subList(2, 1));

                List<String> sub = list.subList(1, 3);
                assertEquals(Arrays.asList("b", "c"), sub);
                assertThrows(UnsupportedOperationException.class, () -> sub.add("x"));
            }
        });
    }

    private static void testPkWithRemoveFlag() {
        withCacheSize(0, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.add("a");
                list.add("b");
                list.add("c");

                assertEquals("b", list.remove(1));
                assertEquals(2, list.size());
                assertEquals(1, list.pk(0));
                assertEquals(3, list.pk(1));
            }
        });
    }

    private static void testRuntimeMetrics() {
        withCacheSize(2, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                var metrics = list.getRuntimeMetrics();
                assertEquals(0, metrics.getCacheHitCount());
                assertEquals(0, metrics.getCacheMissCount());
                assertEquals(0, metrics.getCacheWriteCount());
                assertEquals(0, metrics.getCacheFlushCount());
                assertEquals(0, metrics.getDatabaseWriteOps());

                list.add("a");
                list.add("b");
                assertEquals(2, metrics.getCacheWriteCount());
                assertEquals(2, metrics.getDatabaseSize());

                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
                assertEquals(2, metrics.getCacheHitCount());
                assertEquals(0, metrics.getCacheMissCount());

                list.add("c");
                assertEquals(1, metrics.getCacheFlushCount());
                assertEquals(3, metrics.getDatabaseSize());
                assertTrue(metrics.getCacheFlushTotalNanos() >= 0);
                assertEquals(1, metrics.getDatabaseWriteOps());

                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
                assertEquals("c", list.get(2));
                assertEquals(3, metrics.getCacheMissCount());

                assertEquals("b", list.set(1, "bb"));
                assertEquals(3, metrics.getCacheWriteCount());
                assertEquals(3, metrics.getDatabaseWriteOps());

                list.remove(2);
                assertEquals(2, list.size());
                assertEquals(4, metrics.getDatabaseWriteOps());
            }
        });
    }

    private static void testRuntimeMetricsWithDirectDatabaseWrites() {
        withCacheSize(0, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                var metrics = list.getRuntimeMetrics();

                list.add("a");
                list.add("b");

                assertEquals(0, metrics.getCacheWriteCount());
                assertEquals(0, metrics.getCacheFlushCount());
                assertEquals(2, metrics.getDatabaseWriteOps());
                assertEquals(2, metrics.getDatabaseWriteRows());
                assertEquals(2, metrics.getDatabaseSize());

                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
                assertEquals(0, metrics.getCacheHitCount());
                assertEquals(2, metrics.getCacheMissCount());
                assertEquals(0D, metrics.getCacheHitRate());

                assertEquals("a", list.set(0, "aa"));
                assertEquals(3, metrics.getDatabaseWriteOps());
                assertEquals(3, metrics.getDatabaseWriteRows());
                assertEquals(2, metrics.getDatabaseSize());

                assertEquals("b", list.remove(1));
                assertEquals(4, metrics.getDatabaseWriteOps());
                assertEquals(4, metrics.getDatabaseWriteRows());
                assertEquals(1, metrics.getDatabaseSize());
            }
        });
    }

    private static void testRuntimeMetricsWithFlushIntervalAndChunkConfig() {
        withCacheSize(10, () -> withSystemProperty(CACHE_FLUSH_CHUNK_SIZE_KEY, "2", () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                var metrics = list.getRuntimeMetrics();

                list.add("a");
                list.add("b");
                list.add("c");
                setLastFlushMillis(list, System.currentTimeMillis() - 10);

                withSystemProperty(CACHE_FLUSH_INTERVAL_MILLIS_KEY, "1", () -> list.add("d"));

                assertEquals(4, metrics.getCacheWriteCount());
                assertEquals(1, metrics.getCacheFlushCount());
                assertEquals(1, metrics.getDatabaseWriteOps());
                assertEquals(3, metrics.getDatabaseWriteRows());
                assertEquals(4, metrics.getDatabaseSize());

                assertEquals("a", list.get(0));
                assertEquals(2, metrics.getCacheFlushCount());
                assertEquals(2, metrics.getDatabaseWriteOps());
                assertEquals(4, metrics.getDatabaseWriteRows());
                assertEquals(1, metrics.getCacheMissCount());
                assertEquals(0D, metrics.getCacheHitRate());
            }
        }));
    }

    private static void testSnapshotImportExport() {
        withCacheSize(0, () -> {
            try (LocalList<TestBean1> list = new LocalList<>(TestBean1.class)) {
                list.add(new TestBean1("Jack", 26));
                list.add(new TestBean1("Rose", 25));

                Path json = Files.createTempFile("local-list-snapshot", ".json");
                list.exportToJson(json.toFile());
                list.clear();
                assertEquals(0, list.size());
                list.importFromJson(json.toFile());
                assertEquals(2, list.size());
                assertEquals("Jack", list.get(0).name);
                assertEquals("Rose", list.get(1).name);
                assertEquals(26, list.get(0).age);
                assertEquals(25, list.get(1).age);

                Path csv = Files.createTempFile("local-list-snapshot", ".csv");
                list.exportToCsv(csv.toFile());
                list.clear();
                list.importFromCsv(csv.toFile());
                assertEquals(2, list.size());
                assertEquals("Jack", list.get(0).name);
                assertEquals("Rose", list.get(1).name);
                Files.deleteIfExists(json);
                Files.deleteIfExists(csv);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        withCacheSize(0, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                list.add("a");
                list.add("b");
                Path json = Files.createTempFile("local-list-snapshot-string", ".json");
                list.exportToJson(json.toFile());
                list.clear();
                list.importFromJson(json.toFile());
                assertEquals(2, list.size());
                assertEquals("a", list.get(0));
                assertEquals("b", list.get(1));
                Files.deleteIfExists(json);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void testPluggableSerialization() {
        withCacheSize(0, () -> {
            TypeCodecRegistry.clear();
            try {
                TypeCodec pointCodec = new TypeCodec() {
                    @Override
                    public boolean supports(Class<?> javaType) {
                        return javaType == GeoPoint.class;
                    }

                    @Override
                    public String serialize(Object value) {
                        if (value == null) {
                            return null;
                        }
                        GeoPoint point = (GeoPoint) value;
                        return point.getX() + "," + point.getY();
                    }

                    @Override
                    public Object deserialize(String rawString, Class<?> targetType) {
                        if (rawString == null || rawString.isEmpty()) {
                            return null;
                        }
                        String[] parts = rawString.split(",");
                        return new GeoPoint(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                    }

                    @Override
                    public String getDbType() {
                        return "VARCHAR";
                    }
                };
                TypeCodecRegistry.register(pointCodec);

                try (LocalList<CustomTypeBean> list = new LocalList<>(CustomTypeBean.class)) {
                    list.add(new CustomTypeBean("center", new GeoPoint(10, 20)));
                    list.add(new CustomTypeBean("edge", new GeoPoint(0, -1)));

                    assertEquals("center", list.get(0).name);
                    assertEquals(new GeoPoint(10, 20), list.get(0).point);
                    assertEquals(new GeoPoint(0, -1), list.get(1).point);
                }

                try (LocalMap<String, GeoPoint> map = new LocalMap<>()) {
                    assertNull(map.put("a", new GeoPoint(1, 2)));
                    assertEquals(new GeoPoint(1, 2), map.get("a"));
                    assertEquals("a", map.keySet().iterator().next());
                    assertEquals(new GeoPoint(1, 2), map.put("a", new GeoPoint(3, 4)));
                    assertEquals(new GeoPoint(3, 4), map.get("a"));
                }
            } finally {
                TypeCodecRegistry.clear();
            }
        });
    }

    private static void testRecoveryStateApi() {
        withCacheSize(0, () -> {
            try (LocalList<String> list = new LocalList<>(String.class)) {
                assertFalse(list.isRecoveryRequired());
                list.add("a");
                assertFalse(list.isRecoveryRequired());
                list.recoveryComplete();
                assertFalse(list.isRecoveryRequired());
            }
        });
    }

    private static void withCacheSize(int cacheSize, Runnable runnable) {
        withSystemProperty(CACHE_SIZE_KEY, String.valueOf(cacheSize), runnable);
    }

    private static void withSystemProperty(String key, String value, Runnable runnable) {
        String old = System.getProperty(key);
        System.setProperty(key, value);
        try {
            runnable.run();
        } finally {
            if (old == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, old);
            }
        }
    }

    private static void setLastFlushMillis(LocalList<?> list, long lastFlushMillis) {
        try {
            FieldUtils.writeField(list, "lastFlushMillis", lastFlushMillis, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static void invokeRemoveByKey(LocalList<?> list, String keyColumn, Object key) throws Exception {
        Method m = LocalList.class.getDeclaredMethod("removeByKey", String.class, Object.class);
        m.setAccessible(true);
        m.invoke(list, keyColumn, key);
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomTypeBean {
        private String name;
        private GeoPoint point;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GeoPoint {
        private int x;
        private int y;
    }
}
