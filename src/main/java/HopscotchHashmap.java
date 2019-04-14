import interfaces.HashMapOA;

import java.util.concurrent.locks.ReentrantLock;

public class HopscotchHashmap<V> implements HashMapOA<V> {

    static class HashEntry<V> {
        private volatile int key;
        private volatile V value;
        private ReentrantLock lock;
        //bitmap of records of current hashentry
        private volatile long neiRecBmp;

        HashEntry(int key, V value) {
            this.key = key;
            this.value = value;
            this.lock = new ReentrantLock();
            this.neiRecBmp = 0;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public int getKey() {
            return key;
        }

        public ReentrantLock getLock() {
            return this.lock;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public void setBitmapBit(int index) {
            this.neiRecBmp |= (1 << index);
        }

        public void unsetBitmapBit(int index) {
            this.neiRecBmp &= ~(1 << index);
        }

        public long getNeiRecBmp() {
            return this.neiRecBmp;
        }

        public static void setDeleted(HashEntry entry){
            entry.setKey(DELETED);
            entry.setValue(null);
        }

        public static boolean isFree(HashEntry entry) {
            return (entry == null) || HashEntry.isDeleted(entry);
        }

        private static boolean isDeleted(HashEntry entry) {
            return (entry != null) && (entry.getKey() == DELETED) && (entry.getValue() == null);
        }
    }

    private class FindElemRes {
        private HashEntry he;
        private int index;

        FindElemRes(HashEntry he) {
            this.he = he;
        }

        FindElemRes(HashEntry he, int index) {
            this.he = he;
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public HashEntry getHe() {
            return he;
        }
    }

    private static int NEIGHBOURHOOD_SIZE = 16;

    private static float MAX_LOAD_FACTOR = 0.75f;

    private static int DELETED = Integer.MIN_VALUE + 1;

    private int TABLE_SIZE = 128;

    private volatile int ACTUAL_SIZE = 0;

    HopscotchHashmap.HashEntry[] table;

    HopscotchHashmap() {
        table = new HopscotchHashmap.HashEntry[TABLE_SIZE + NEIGHBOURHOOD_SIZE];
        //todo refactor memcopy ? stream
        for (int i = 0; i < TABLE_SIZE + NEIGHBOURHOOD_SIZE; i++)
            table[i] = null;
    }

    //    private boolean isNull(HashEntry entry) {
//        //todo not sure if it is correct in multithreaded_environment
//        return entry == null;
//    }

    /**
     * hash fuction
     *
     * @param key
     */
    private int h(int key) {
        return key & (TABLE_SIZE - 1);
        //return key % TABLE_SIZE;
        //return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
        //((key) & (MAX_SEGMENTS-1));
    }

    @Override
    public V put(int key, V value) {
        if ((key == DELETED) && (value ==null)) {
            throw new IllegalArgumentException("Key cant be "+DELETED+" and value cant be NULL");
        }
        int bucket_index = h(key);
        HashEntry bucket = table[bucket_index];
        int i, mask = 1;
        long bitmap = bucket.getNeiRecBmp();
        HashEntry<V> entry = null;
        for (i = 0; i < NEIGHBOURHOOD_SIZE; ++i, mask <<= 1) {
            entry = table[bucket_index + i];
            if (entry == null) {
                table[bucket_index + i] = new HashEntry(key, value);
                bucket.setBitmapBit(i);
                return null;//not equals cos primitive
            } else if (HashEntry.isDeleted(entry)) {
                //return previous value if necessary
                entry.setKey(key);
                entry.setValue(value);
                bucket.setBitmapBit(i);
                return null;
            } else if (entry.getKey() == key) {
                //return previous value if necessary
                V prevValue = entry.getValue();
                entry.setValue(value);
                return prevValue;
            }
        }
    }


    @Override
    public V remove(int key) {
        int bucket_index = h(key);
        HashEntry<V> found_bucket, bucket = table[bucket_index];
        //bucketnull
        if (bucket == null) return null;
        bucket.getLock().lock();//
        FindElemRes foundRes = findElem(bucket, bucket_index, key);
        found_bucket = foundRes.getHe();
        if (found_bucket != null) {
            V value = found_bucket.getValue();
            HashEntry.setDeleted(found_bucket);
            bucket.unsetBitmapBit(foundRes.getIndex());
            ACTUAL_SIZE--;
            bucket.getLock().unlock();//
            return value;
        } else {
            bucket.getLock().unlock();//
            return null;
        }
    }

    @Override
    public boolean containsKey(int key) {
        int bucket_index = h(key);
        HashEntry bucket = table[bucket_index];
        //bucketnull
        if (bucket == null) return false;
        bucket = findElem(bucket, bucket_index, key).getHe();
        return (bucket != null);
    }

    private FindElemRes findElem(HashEntry bucket, int init_bucket_index, int key) {
        long bitmap = bucket.getNeiRecBmp();
        long mask = 1;
        int i;
        HashEntry check_bucket = null;
        for (i = 0; i < NEIGHBOURHOOD_SIZE; ++i, mask <<= 1) {
            if ((mask & bitmap) >= 1) {
                check_bucket = table[init_bucket_index + i];
                if (key == check_bucket.getKey()) {
                    break;
                }
            }
        }
        if (i >= NEIGHBOURHOOD_SIZE) {
            return new FindElemRes(null, i);
        } else {
            return new FindElemRes(check_bucket, i);
        }
    }

    @Override
    public int size() {
        return ACTUAL_SIZE;
    }

    @Override
    public V get(int key) {
        int bucket_index = h(key);
        HashEntry<V> bucket = table[bucket_index];
        //bucketnull
        if (bucket == null) return null;
        bucket = findElem(bucket, bucket_index, key).getHe();
        if (bucket != null) {
            return bucket.getValue();
        } else {
            return null;
        }
    }
}
