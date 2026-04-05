package datahike.java;

import clojure.lang.*;
import java.util.*;

/**
 * Immutable query result set backed by an Object[] of PersistentVectors.
 *
 * Extends APersistentSet to get IFn (set-as-function), java.util.Set,
 * hashCode/equals, toString, etc. for free.
 *
 * Fast path: seq/count/iterate operate directly on the backing array.
 * Slow path: contains/get lazily build a j.u.HashSet index on first call.
 * Mutation path: cons/disjoin materialize to PersistentHashSet (rare).
 *
 * Designed for fused-scan query results where uniqueness is guaranteed
 * by the scan, so no dedup is needed at construction time.
 */
public class QueryResult extends APersistentSet implements IObj {
    private final Object[] items;
    private final int _size;
    private final IPersistentMap _meta;
    // Lazily built on first contains/get call
    private volatile HashSet<Object> index;
    // Local hash caches (APersistentSet._hash/_hasheq are package-private)
    private int cachedHash;
    private int cachedHasheq;

    /**
     * Create a QueryResult from an array of results (PersistentVectors).
     * The array is NOT copied — caller must not mutate it after construction.
     */
    public QueryResult(Object[] items, int size) {
        super(PersistentArrayMap.EMPTY); // impl not used — all methods overridden
        this.items = items;
        this._size = size;
        this._meta = null;
    }

    public QueryResult(Object[] items) {
        this(items, items.length);
    }

    private QueryResult(IPersistentMap meta, Object[] items, int size) {
        super(PersistentArrayMap.EMPTY);
        this.items = items;
        this._size = size;
        this._meta = meta;
    }

    @Override
    public IObj withMeta(IPersistentMap meta) {
        if (meta == _meta) return this;
        return new QueryResult(meta, items, _size);
    }

    @Override
    public IPersistentMap meta() {
        return _meta;
    }

    // --- Core IPersistentSet ---

    @Override
    public int count() {
        return _size;
    }

    @Override
    public ISeq seq() {
        if (_size == 0) return null;
        return new Seq(items, _size, 0);
    }

    @Override
    public boolean contains(Object key) {
        return ensureIndex().contains(key);
    }

    @Override
    public Object get(Object key) {
        if (ensureIndex().contains(key))
            return key;
        return null;
    }

    @Override
    public IPersistentSet disjoin(Object key) {
        return materialize().disjoin(key);
    }

    @Override
    public IPersistentCollection cons(Object o) {
        return materialize().cons(o);
    }

    @Override
    public IPersistentCollection empty() {
        return PersistentHashSet.EMPTY;
    }

    @Override
    public boolean equiv(Object o) {
        return setEquals(this, o);
    }

    // --- Hashing ---

    @Override
    public int hashCode() {
        if (cachedHash == 0 && _size > 0) {
            int h = 0;
            for (int i = 0; i < _size; i++) {
                h += items[i].hashCode();
            }
            cachedHash = h;
        }
        return cachedHash;
    }

    @Override
    public int hasheq() {
        if (cachedHasheq == 0 && _size > 0) {
            int h = 0;
            for (int i = 0; i < _size; i++) {
                h += Murmur3.hashInt(items[i].hashCode());
            }
            cachedHasheq = Murmur3.mixCollHash(h, _size);
        }
        return cachedHasheq;
    }

    // --- java.util.Set / Collection ---

    @Override
    public Iterator iterator() {
        return new Iter(items, _size);
    }

    @Override
    public int size() {
        return _size;
    }

    @Override
    public boolean isEmpty() {
        return _size == 0;
    }

    @Override
    public Object[] toArray() {
        return Arrays.copyOf(items, _size);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] toArray(Object[] a) {
        if (a.length < _size)
            return Arrays.copyOf(items, _size, a.getClass());
        System.arraycopy(items, 0, a, 0, _size);
        if (a.length > _size)
            a[_size] = null;
        return a;
    }

    // --- Internals ---

    private HashSet<Object> ensureIndex() {
        if (index == null) {
            synchronized (this) {
                if (index == null) {
                    HashSet<Object> s = new HashSet<>(_size * 2);
                    for (int i = 0; i < _size; i++) {
                        s.add(items[i]);
                    }
                    index = s;
                }
            }
        }
        return index;
    }

    private PersistentHashSet materialize() {
        ITransientSet ts = (ITransientSet) PersistentHashSet.EMPTY.asTransient();
        for (int i = 0; i < _size; i++) {
            ts = (ITransientSet) ts.conj(items[i]);
        }
        return (PersistentHashSet) ts.persistent();
    }

    // --- ISeq for array ---

    private static class Seq extends ASeq implements Counted {
        private final Object[] items;
        private final int size;
        private final int idx;

        Seq(Object[] items, int size, int idx) {
            this.items = items;
            this.size = size;
            this.idx = idx;
        }

        Seq(IPersistentMap meta, Object[] items, int size, int idx) {
            super(meta);
            this.items = items;
            this.size = size;
            this.idx = idx;
        }

        @Override
        public Object first() {
            return items[idx];
        }

        @Override
        public ISeq next() {
            int next = idx + 1;
            if (next < size)
                return new Seq(items, size, next);
            return null;
        }

        @Override
        public Obj withMeta(IPersistentMap meta) {
            return new Seq(meta, items, size, idx);
        }

        @Override
        public int count() {
            return size - idx;
        }
    }

    // --- Iterator ---

    private static class Iter implements Iterator<Object> {
        private final Object[] items;
        private final int size;
        private int idx;

        Iter(Object[] items, int size) {
            this.items = items;
            this.size = size;
            this.idx = 0;
        }

        @Override
        public boolean hasNext() {
            return idx < size;
        }

        @Override
        public Object next() {
            if (idx >= size) throw new NoSuchElementException();
            return items[idx++];
        }
    }
}
