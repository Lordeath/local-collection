package lordeath.local.collection;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * LocalMap线程安全包装类。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class SynchronizedLocalMap<K extends String, V> implements Map<K, V>, AutoCloseable {
    private final LocalMap<K, V> delegate;
    private final Object mutex;

    public SynchronizedLocalMap(LocalMap<K, V> delegate) {
        this(delegate, new Object());
    }

    public SynchronizedLocalMap(LocalMap<K, V> delegate, Object mutex) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.mutex = Objects.requireNonNull(mutex, "mutex is null");
    }

    public LocalMap<K, V> getDelegate() {
        return delegate;
    }

    public Object getMutex() {
        return mutex;
    }

    @Override
    public int size() {
        synchronized (mutex) {
            return delegate.size();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (mutex) {
            return delegate.isEmpty();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        synchronized (mutex) {
            return delegate.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        synchronized (mutex) {
            return delegate.containsValue(value);
        }
    }

    @Override
    public V get(Object key) {
        synchronized (mutex) {
            return delegate.get(key);
        }
    }

    @Override
    public V put(K key, V value) {
        synchronized (mutex) {
            return delegate.put(key, value);
        }
    }

    @Override
    public V remove(Object key) {
        synchronized (mutex) {
            return delegate.remove(key);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        synchronized (mutex) {
            delegate.putAll(m);
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            delegate.clear();
        }
    }

    @Override
    public Set<K> keySet() {
        synchronized (mutex) {
            return new LinkedHashSet<>(delegate.keySet());
        }
    }

    @Override
    public Collection<V> values() {
        synchronized (mutex) {
            return new ArrayList<>(delegate.values());
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        synchronized (mutex) {
            return new LinkedHashSet<>(delegate.entrySet());
        }
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        synchronized (mutex) {
            delegate.forEach(action);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        synchronized (mutex) {
            return delegate.remove(key, value);
        }
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        synchronized (mutex) {
            return delegate.getOrDefault(key, defaultValue);
        }
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        synchronized (mutex) {
            delegate.replaceAll(function);
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        synchronized (mutex) {
            return delegate.putIfAbsent(key, value);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        synchronized (mutex) {
            return delegate.replace(key, oldValue, newValue);
        }
    }

    @Override
    public V replace(K key, V value) {
        synchronized (mutex) {
            return delegate.replace(key, value);
        }
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        synchronized (mutex) {
            return delegate.computeIfAbsent(key, mappingFunction);
        }
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        synchronized (mutex) {
            return delegate.computeIfPresent(key, remappingFunction);
        }
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        synchronized (mutex) {
            return delegate.compute(key, remappingFunction);
        }
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        synchronized (mutex) {
            return delegate.merge(key, value, remappingFunction);
        }
    }

    @Override
    public void close() {
        synchronized (mutex) {
            delegate.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        synchronized (mutex) {
            return delegate.equals(o);
        }
    }

    @Override
    public int hashCode() {
        synchronized (mutex) {
            return delegate.hashCode();
        }
    }

    @Override
    public String toString() {
        synchronized (mutex) {
            return delegate.toString();
        }
    }
}
