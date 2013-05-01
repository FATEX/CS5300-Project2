package project2;

import java.lang.ref.SoftReference;
import java.util.LinkedHashMap;


//Taken from http://stackoverflow.com/questions/862648/soft-reference-linkedhashmap-in-java
public class SoftLinkedCache<K, V>
{
    private static final long serialVersionUID = -4585400640420886743L;

    private final LinkedHashMap<K, SoftReference<V>> map;

    public SoftLinkedCache(final int cacheSize)
    {
        if (cacheSize < 1)
            throw new IllegalArgumentException("cache size must be greater than 0");

        map = new LinkedHashMap<K, SoftReference<V>>()
        {
            private static final long serialVersionUID = 5857390063785416719L;

            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<K, SoftReference<V>> eldest)
            {
                return size() > cacheSize;
            }
        };
    }

    public synchronized V put(K key, V value)
    {
        SoftReference<V> previousValueReference = map.put(key, new SoftReference<V>(value));
        return previousValueReference != null ? previousValueReference.get() : null;
    }

    public synchronized V get(K key)
    {
        SoftReference<V> valueReference = map.get(key);
        return valueReference != null ? valueReference.get() : null;
    }
}