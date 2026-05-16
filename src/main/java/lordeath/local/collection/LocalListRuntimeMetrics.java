package lordeath.local.collection;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LocalList 的运行时指标快照。
 */
public final class LocalListRuntimeMetrics {

    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();
    private final LongAdder cacheWrites = new LongAdder();
    private final LongAdder cacheFlushCount = new LongAdder();
    private final LongAdder cacheFlushTimeNanos = new LongAdder();
    private final LongAdder databaseWriteOps = new LongAdder();
    private final LongAdder databaseWriteRows = new LongAdder();
    private final AtomicLong databaseSize = new AtomicLong(0);

    void recordCacheHit() {
        cacheHits.increment();
    }

    void recordCacheMiss() {
        cacheMisses.increment();
    }

    void recordCacheWrite() {
        cacheWrites.increment();
    }

    void recordCacheWrite(int count) {
        if (count > 0) {
            cacheWrites.add(count);
        }
    }

    void recordCacheFlush(int flushSize, long elapsedNanos) {
        if (flushSize <= 0) {
            return;
        }
        cacheFlushCount.increment();
        cacheFlushTimeNanos.add(elapsedNanos);
    }

    void recordDatabaseWrite(int rows) {
        if (rows <= 0) {
            return;
        }
        databaseWriteOps.increment();
        databaseWriteRows.add(rows);
    }

    public long getCacheHitCount() {
        return cacheHits.sum();
    }

    public long getCacheMissCount() {
        return cacheMisses.sum();
    }

    public double getCacheHitRate() {
        long total = getCacheHitCount() + getCacheMissCount();
        return total == 0 ? 0D : (double) getCacheHitCount() / total;
    }

    public long getCacheWriteCount() {
        return cacheWrites.sum();
    }

    public long getCacheFlushCount() {
        return cacheFlushCount.sum();
    }

    public long getCacheFlushTotalNanos() {
        return cacheFlushTimeNanos.sum();
    }

    public long getDatabaseWriteOps() {
        return databaseWriteOps.sum();
    }

    public long getDatabaseWriteRows() {
        return databaseWriteRows.sum();
    }

    public long getDatabaseSize() {
        return databaseSize.get();
    }

    void recordDatabaseSize(long size) {
        if (size >= 0) {
            databaseSize.set(size);
        }
    }
}
