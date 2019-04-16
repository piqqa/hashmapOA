package com.company;

import com.company.exception.HashmapOversizeException;
import com.company.interfaces.HashMapOA;

import java.util.concurrent.locks.ReentrantLock;

public class HopscotchHashmap<V> implements HashMapOA<V> {

    static class HashEntry<V> {
        private volatile int key;
        private volatile V value;
        private ReentrantLock lock;
        //bitmap of records of current hashentry
        private volatile long neiRecBmp;

        HashEntry() {
            this.setKey(DELETED);
            this.setValue(null);
            this.lock = new ReentrantLock();
            this.neiRecBmp = 0;
        }

        HashEntry(int key, V value) {
            this.key = key;
            this.value = value;
            this.lock = new ReentrantLock();
            this.neiRecBmp = 0;
        }

        @Override
        public String toString() {
            return "HashEntry{" +
                    "key=" + key +
                    ", value=" + value +
                    ", bitmap=" + Long.toBinaryString(neiRecBmp) +
                    '}';
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

        public static void setDeleted(HashEntry entry) {
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

    private class BucketInfo {
        private int free_bucket_index;
        private int free_distance;

        public BucketInfo(int free_bucket_index, int free_distance) {
            this.free_bucket_index = free_bucket_index;
            this.free_distance = free_distance;
        }

        public int getFreeBucketIndex() {
            return free_bucket_index;
        }

        public void setFreeBucketIndex(int free_bucket_index) {
            this.free_bucket_index = free_bucket_index;
        }

        public int getFreeDistance() {
            return free_distance;
        }

        public void setFreeDistance(int free_distance) {
            this.free_distance = free_distance;
        }
    }

    final static int MAX_CAPACITY = 1048576; // Including neighbourhood for last hash location

    private static int NEIGHBOURHOOD_SIZE = 16;

    private static float MAX_LOAD_FACTOR = 0.75f;

    private static int DELETED = Integer.MIN_VALUE + 1;

    private int TABLE_SIZE = 128;

    private volatile int ACTUAL_SIZE = 0;

    private ReentrantLock tableLock = new ReentrantLock();

    HopscotchHashmap.HashEntry<V>[] table;

    public HopscotchHashmap() {
        this(128);
    }

    public HopscotchHashmap(int set_capacity) {
        if (set_capacity < 16) {
            TABLE_SIZE = 16;
        } else if (set_capacity > MAX_CAPACITY) {
            TABLE_SIZE = MAX_CAPACITY;
        } else {
            TABLE_SIZE = set_capacity;
        }
        table = new HopscotchHashmap.HashEntry[TABLE_SIZE];
        //todo refactor memcopy ? stream
        for (int i = 0; i < TABLE_SIZE; i++)
            table[i] = null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Size :" + ACTUAL_SIZE + "\n");
        for (int i = 0; i < TABLE_SIZE; i++) {
            if (/*!HashEntry.isFree(table[i])*/table[i] != null) {
                sb.append("Index[").append(i).append("] -- ").append(table[i]).append("\n");
            }
        }
        return sb.toString();
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

    private void resize() throws HashmapOversizeException {
        tableLock.lock();
        int prevTableSize = TABLE_SIZE;
        if (TABLE_SIZE == MAX_CAPACITY) {
            tableLock.unlock();
            throw new HashmapOversizeException("Size of hashmap is exceeded");
        }
        TABLE_SIZE <<= 1;
        if (TABLE_SIZE > MAX_CAPACITY) {
            TABLE_SIZE = MAX_CAPACITY;
        }
        HashEntry[] oldTable = table;
        table = new HashEntry[TABLE_SIZE];
        ACTUAL_SIZE = 0;
        for (int i = 0; i < prevTableSize; i++) {
            HashEntry<V> entry = oldTable[i];
            if (entry != null && !HashEntry.isDeleted(entry)) {
                put(entry.getKey(), entry.getValue());
            }
        }
        tableLock.unlock();
    }

//    HashEntry<V> initBucket(int index){
//
//    }

    @Override
    public V put(int key, V value) {
        //todo for test use only movge to signature in prod
        try {
            if (!tableLock.isHeldByCurrentThread()) {
                tableLock.lock();
                tableLock.unlock();
            }
            if ((key == DELETED) && (value == null)) {
                throw new IllegalArgumentException("Key cant be " + DELETED + " and value cant be NULL");
            }
            int bucket_index = h(key);
            HashEntry bucket = table[bucket_index];
            if (bucket == null) {
                table[bucket_index] = new HashEntry();
                bucket = table[bucket_index];
            }
            bucket.getLock().lock();
            int i;
            long bitmap = bucket.getNeiRecBmp();
            HashEntry<V> entry = null;
            V res = null;
            for (i = 0; i < NEIGHBOURHOOD_SIZE; ++i) {
                entry = table[ali(bucket_index + i)];
                boolean success = false;
                if (entry == null) {
                    table[ali(bucket_index + i)] = new HashEntry(key, value);
                    bucket.setBitmapBit(i);
                    ++ACTUAL_SIZE;
                    success = true;
                } else if (HashEntry.isDeleted(entry)) {
                    //return previous value if necessary
                    entry.setKey(key);
                    entry.setValue(value);
                    bucket.setBitmapBit(i);
                    ++ACTUAL_SIZE;
                    success = true;
                } else if (entry.getKey() == key) {
                    //return previous value if necessary
                    V prevValue = entry.getValue();
                    entry.setValue(value);
                    res = prevValue;
                    success = true;
                }
                if (success) {
                    bucket.getLock().unlock();
                    if (ACTUAL_SIZE * 1.0 / TABLE_SIZE > MAX_LOAD_FACTOR)
                        resize();
                    return res;
                }
            }
            int free_bucket_index = -1, free_distance;
            for (; i < TABLE_SIZE; ++i) {
                entry = table[ali(bucket_index + i)];
                if (entry == null || HashEntry.isDeleted(entry)) {
                    free_bucket_index = bucket_index + i;
                    break;
                }
            }
            //com.company.hopscotch itself
            if (i < TABLE_SIZE) {
                free_distance = i;
                BucketInfo found_closest_bucket;
                found_closest_bucket = find_closer_bucket(new BucketInfo(free_bucket_index, free_distance));
                while (found_closest_bucket != null) {
                    free_distance = found_closest_bucket.getFreeDistance();
                    free_bucket_index = found_closest_bucket.getFreeBucketIndex();
                    entry = table[ali(free_bucket_index)];
                    //found
                    if (free_distance < NEIGHBOURHOOD_SIZE) {
                        entry.setKey(key);
                        entry.setValue(value);
                        bucket.setBitmapBit(i);
                        ++ACTUAL_SIZE;
                        bucket.getLock().unlock();
                        if (ACTUAL_SIZE * 1.0 / TABLE_SIZE > MAX_LOAD_FACTOR)
                            resize();
                        return null;
                    }
                    found_closest_bucket = find_closer_bucket(new BucketInfo(free_bucket_index, free_distance));
                }
            }
            resize();
        } catch (HashmapOversizeException he) {
            System.out.println(he);
        }
        //recall after resize
        return put(key, value);
    }

    BucketInfo find_closer_bucket(BucketInfo bi) {
        int free_bucket_index = bi.getFreeBucketIndex();
        HashEntry<V> free_bucket = table[ali(free_bucket_index)];
        //todo extract init empty to func
        if (free_bucket == null) {
            table[ali(free_bucket_index)] = new HashEntry();
            free_bucket = table[ali(free_bucket_index)];
        }
        int free_distance = bi.getFreeDistance();
        //0 - free distance, 1 - val, 2 - new free bucket
        BucketInfo result = new BucketInfo(0, 0);
        int move_bucket_index = free_bucket_index - (NEIGHBOURHOOD_SIZE - 1);
        HashEntry<V> move_bucket = table[ali(move_bucket_index)];

        for (int free_dist = (NEIGHBOURHOOD_SIZE - 1); free_dist > 0; --free_dist) {
            long start_hop_info = move_bucket.getNeiRecBmp();
            int move_free_distance = -1;
            long mask = 1;
            for (int i = 0; i < free_dist; ++i, mask <<= 1) {
                if ((mask & start_hop_info) >= 1) {
                    move_free_distance = i;
                    break;
                }
            }
            /*When a suitable bucket is found, it's content is moved to the old free_bucket*/
            if (-1 != move_free_distance) {
                move_bucket.getLock().lock();
                if (start_hop_info == move_bucket.getNeiRecBmp()) {
                    int new_free_bucket_index = move_bucket_index + move_free_distance;
                    HashEntry<V> new_free_bucket = table[ali(new_free_bucket_index)];
                    if (new_free_bucket == null) {
                        table[ali(new_free_bucket_index)] = new HashEntry();
                        new_free_bucket = table[ali(new_free_bucket_index)];
                    }
                    /*Updates move_bucket's hop_info, to indicate the newly inserted bucket*/
                    move_bucket.setBitmapBit(free_dist);
                    free_bucket.setValue(new_free_bucket.getValue());
                    free_bucket.setKey(new_free_bucket.getKey());

                    HashEntry.setDeleted(new_free_bucket);
                    /*Updates move_bucket's hop_info, to indicate the deleted bucket*/
                    move_bucket.unsetBitmapBit(move_free_distance);

                    free_distance = free_distance - free_dist + move_free_distance;
                    move_bucket.getLock().unlock();
                    result.setFreeDistance(free_distance);
                    result.setFreeBucketIndex(new_free_bucket_index);
                    return result;
                }
                move_bucket.getLock().unlock();
            }
            ++move_bucket_index;
            move_bucket = table[ali(move_bucket_index)];
        }
        return null;
    }

    @Override
    public V remove(int key) {
        //todo check that not resize
        tableLock.lock();
        tableLock.unlock();
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
            //todo unlock in finally
            bucket.getLock().unlock();//
            return value;
        } else {
            bucket.getLock().unlock();//
            return null;
        }
    }

    @Override
    public boolean containsKey(int key) {
        tableLock.lock();
        tableLock.unlock();
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
                check_bucket = table[ali(init_bucket_index + i)];
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

    /**
     * Зациклим индекс, если он вне пределов таблицы
     *
     * @param index
     * @return
     */
    private int ali(int index) {
        int result = index & (TABLE_SIZE - 1);
        return result;
    }

    @Override
    public int size() {
        return ACTUAL_SIZE;
    }

    @Override
    public V get(int key) {
        tableLock.lock();
        tableLock.unlock();
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
