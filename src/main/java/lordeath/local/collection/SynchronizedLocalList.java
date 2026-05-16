package lordeath.local.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.io.File;

/**
 * LocalList线程安全包装类。
 *
 * @param <E> 元素类型
 */
public final class SynchronizedLocalList<E> implements List<E>, AutoCloseable {
    private final LocalList<E> delegate;
    private final Object mutex;

    public SynchronizedLocalList(LocalList<E> delegate) {
        this(delegate, new Object());
    }

    public SynchronizedLocalList(LocalList<E> delegate, Object mutex) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.mutex = Objects.requireNonNull(mutex, "mutex is null");
    }

    public LocalList<E> getDelegate() {
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
    public boolean contains(Object o) {
        synchronized (mutex) {
            return delegate.contains(o);
        }
    }

    @Override
    public Iterator<E> iterator() {
        synchronized (mutex) {
            return new SynchronizedIterator(delegate.iterator());
        }
    }

    @Override
    public Object[] toArray() {
        synchronized (mutex) {
            return delegate.toArray();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        synchronized (mutex) {
            return delegate.toArray(a);
        }
    }

    @Override
    public boolean add(E e) {
        synchronized (mutex) {
            return delegate.add(e);
        }
    }

    @Override
    public boolean remove(Object o) {
        synchronized (mutex) {
            return delegate.remove(o);
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        synchronized (mutex) {
            return delegate.containsAll(c);
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        synchronized (mutex) {
            return delegate.addAll(c);
        }
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        synchronized (mutex) {
            return delegate.addAll(index, c);
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        synchronized (mutex) {
            return delegate.removeAll(c);
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        synchronized (mutex) {
            return delegate.retainAll(c);
        }
    }

    @Override
    public void clear() {
        synchronized (mutex) {
            delegate.clear();
        }
    }

    public boolean isRecoveryRequired() {
        synchronized (mutex) {
            return delegate.isRecoveryRequired();
        }
    }

    public void recoveryComplete() {
        synchronized (mutex) {
            delegate.recoveryComplete();
        }
    }

    public void exportToJson(File snapshotFile) {
        synchronized (mutex) {
            delegate.exportToJson(snapshotFile);
        }
    }

    public void importFromJson(File snapshotFile) {
        synchronized (mutex) {
            delegate.importFromJson(snapshotFile);
        }
    }

    public void exportToCsv(File snapshotFile) {
        synchronized (mutex) {
            delegate.exportToCsv(snapshotFile);
        }
    }

    public void importFromCsv(File snapshotFile) {
        synchronized (mutex) {
            delegate.importFromCsv(snapshotFile);
        }
    }

    @Override
    public E get(int index) {
        synchronized (mutex) {
            return delegate.get(index);
        }
    }

    @Override
    public E set(int index, E element) {
        synchronized (mutex) {
            return delegate.set(index, element);
        }
    }

    @Override
    public void add(int index, E element) {
        synchronized (mutex) {
            delegate.add(index, element);
        }
    }

    @Override
    public E remove(int index) {
        synchronized (mutex) {
            return delegate.remove(index);
        }
    }

    @Override
    public int indexOf(Object o) {
        synchronized (mutex) {
            return delegate.indexOf(o);
        }
    }

    @Override
    public int lastIndexOf(Object o) {
        synchronized (mutex) {
            return delegate.lastIndexOf(o);
        }
    }

    @Override
    public ListIterator<E> listIterator() {
        synchronized (mutex) {
            return new SynchronizedListIterator(delegate.listIterator());
        }
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        synchronized (mutex) {
            return new SynchronizedListIterator(delegate.listIterator(index));
        }
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        synchronized (mutex) {
            return new ArrayList<>(delegate.subList(fromIndex, toIndex));
        }
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        synchronized (mutex) {
            delegate.forEach(action);
        }
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        synchronized (mutex) {
            delegate.replaceAll(operator);
        }
    }

    @Override
    public void sort(Comparator<? super E> c) {
        synchronized (mutex) {
            delegate.sort(c);
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

    private final class SynchronizedIterator implements Iterator<E> {
        private final Iterator<E> iterator;

        private SynchronizedIterator(Iterator<E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            synchronized (mutex) {
                return iterator.hasNext();
            }
        }

        @Override
        public E next() {
            synchronized (mutex) {
                return iterator.next();
            }
        }

        @Override
        public void remove() {
            synchronized (mutex) {
                iterator.remove();
            }
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            synchronized (mutex) {
                iterator.forEachRemaining(action);
            }
        }
    }

    private final class SynchronizedListIterator implements ListIterator<E> {
        private final ListIterator<E> listIterator;

        private SynchronizedListIterator(ListIterator<E> listIterator) {
            this.listIterator = listIterator;
        }

        @Override
        public boolean hasNext() {
            synchronized (mutex) {
                return listIterator.hasNext();
            }
        }

        @Override
        public E next() {
            synchronized (mutex) {
                return listIterator.next();
            }
        }

        @Override
        public boolean hasPrevious() {
            synchronized (mutex) {
                return listIterator.hasPrevious();
            }
        }

        @Override
        public E previous() {
            synchronized (mutex) {
                return listIterator.previous();
            }
        }

        @Override
        public int nextIndex() {
            synchronized (mutex) {
                return listIterator.nextIndex();
            }
        }

        @Override
        public int previousIndex() {
            synchronized (mutex) {
                return listIterator.previousIndex();
            }
        }

        @Override
        public void remove() {
            synchronized (mutex) {
                listIterator.remove();
            }
        }

        @Override
        public void set(E e) {
            synchronized (mutex) {
                listIterator.set(e);
            }
        }

        @Override
        public void add(E e) {
            synchronized (mutex) {
                listIterator.add(e);
            }
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            synchronized (mutex) {
                listIterator.forEachRemaining(action);
            }
        }
    }
}
