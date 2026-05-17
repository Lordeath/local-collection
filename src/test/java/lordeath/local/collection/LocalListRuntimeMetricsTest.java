package lordeath.local.collection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalListRuntimeMetricsTest {

    @Test
    void cacheHitRateDefaultsToZeroWhenNoReadsWereRecorded() {
        LocalListRuntimeMetrics metrics = new LocalListRuntimeMetrics();

        assertEquals(0, metrics.getCacheHitCount());
        assertEquals(0, metrics.getCacheMissCount());
        assertEquals(0D, metrics.getCacheHitRate());
    }

    @Test
    void cacheHitRateUsesHitsAndMisses() {
        LocalListRuntimeMetrics metrics = new LocalListRuntimeMetrics();

        metrics.recordCacheHit();
        metrics.recordCacheHit();
        metrics.recordCacheMiss();

        assertEquals(2, metrics.getCacheHitCount());
        assertEquals(1, metrics.getCacheMissCount());
        assertEquals(2D / 3D, metrics.getCacheHitRate());
    }

    @Test
    void ignoresNonPositiveBulkCounters() {
        LocalListRuntimeMetrics metrics = new LocalListRuntimeMetrics();

        metrics.recordCacheWrite(0);
        metrics.recordCacheWrite(-1);
        metrics.recordCacheFlush(0, 100);
        metrics.recordCacheFlush(-1, 100);
        metrics.recordDatabaseWrite(0);
        metrics.recordDatabaseWrite(-1);

        assertEquals(0, metrics.getCacheWriteCount());
        assertEquals(0, metrics.getCacheFlushCount());
        assertEquals(0, metrics.getCacheFlushTotalNanos());
        assertEquals(0, metrics.getDatabaseWriteOps());
        assertEquals(0, metrics.getDatabaseWriteRows());
    }

    @Test
    void recordsFlushAndDatabaseWriteTotals() {
        LocalListRuntimeMetrics metrics = new LocalListRuntimeMetrics();

        metrics.recordCacheWrite();
        metrics.recordCacheWrite(2);
        metrics.recordCacheFlush(3, 42);
        metrics.recordDatabaseWrite(3);
        metrics.recordDatabaseSize(3);
        metrics.recordDatabaseSize(-1);

        assertEquals(3, metrics.getCacheWriteCount());
        assertEquals(1, metrics.getCacheFlushCount());
        assertEquals(42, metrics.getCacheFlushTotalNanos());
        assertEquals(1, metrics.getDatabaseWriteOps());
        assertEquals(3, metrics.getDatabaseWriteRows());
        assertEquals(3, metrics.getDatabaseSize());
    }
}
