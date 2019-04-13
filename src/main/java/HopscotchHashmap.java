import interfaces.HashMapOAInt;

import java.util.concurrent.locks.ReentrantLock;

public class HopscotchHashmap<V> implements HashMapOAInt<V> {

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
            this.neiRecBmp=0;
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

        public void setKey(int key) {
            this.key = key;
        }

        public void setBitmapBit(int index){
            this.neiRecBmp |= (1<<index);
        }

        public void unsetBitmapBit(int index){
            this.neiRecBmp &= ~(1<<index);
        }

        public long getNeiRecBmp(){
            return this.neiRecBmp;
        }
    }

    private static int NEIGHBOURHOOD_SIZE = 16;

    private static float MAX_LOAD_FACTOR = 0.75f;

    private static int DELETED = -1;

    private int TABLE_SIZE = 128;

    private volatile int ACTUAL_SIZE = 0;

    HopscotchHashmap.HashEntry[] table;

    HopscotchHashmap(){
        table = new HopscotchHashmap.HashEntry[TABLE_SIZE];
        //todo refactor memcopy ? stream
        for (int i = 0; i < TABLE_SIZE; i++)
            table[i] = null;
    }

    private boolean isFree(HashEntry entry) {
        //todo not sure if it is correct in multithreaded_environment
        return (entry == null) || (entry.getKey() != DELETED);
    }

    //    private boolean isNull(HashEntry entry) {
//        //todo not sure if it is correct in multithreaded_environment
//        return entry == null;
//    }

    /**
     * hash fuction
     * @param key
     */
    private int h(int key){
        return key % TABLE_SIZE;
    }

    private boolean tryInsertSet(HashEntry entry, int key, V value) {
        if (isFree(entry)) {
            entry = new HashEntry(key, value);
            return true;//not equals cos primitive
        } else if (entry.getKey() == key) {
            //return previous value if necessary
            entry.setValue(value);
            return true;
        }
        return false;
    }

    @Override
    public boolean add(int key, V value) {
        int initialHash = h(key);
        int hash = initialHash;
        int i = 0;
        //try {
        //HashEntry ref=table[hash];
        boolean haveSet = false;
        do {
            haveSet = tryInsertSet(table[hash], key, value);
            ++i;
        }
        while (!haveSet && (i < TABLE_SIZE));//or do while(...hash!=initialHash)

//        } catch (NullPointerException e) {
//            table[h] = new Par(x, y);
//            return null;
//        }
    }


    @Override
    public V remove(int key) {
        int bucket_index = h(key);
        HashEntry<V> bucket = table[bucket_index];
        bucket.lock.lock();//
        long bitmap = bucket.getNeiRecBmp();
        long mask = 1;
        for (int i = 0; i < NEIGHBOURHOOD_SIZE; ++i, mask <<= 1) {
            if((mask & bitmap) >= 1) {
                HashEntry<V> check_bucket = table[bucket_index+i];
                if(key == check_bucket.getKey()) {
                    V rc = check_bucket.getValue();
                    check_bucket.setValue(null);
                    check_bucket.setKey(-1);
                    bucket.unsetBitmapBit(i);
                    bucket.lock.unlock();//
                    return rc;
                }
            }
        }
        bucket.lock.unlock();//
        return null;
    }

    @Override
    public boolean containsKey(int key) {
        int bucket_index = h(key);
        HashEntry bucket = table[bucket_index];
        long bitmap = bucket.getNeiRecBmp();
        long mask = 1;
        for (int i = 0; i < NEIGHBOURHOOD_SIZE; ++i, mask <<= 1) {
            if((mask & bitmap) >= 1) {
                HashEntry check_bucket = table[bucket_index+i];
                if(key == check_bucket.getKey()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int size() {
        return ACTUAL_SIZE;
    }

    @Override
    public V get(int key) {
        int bucket_index = h(key);
        HashEntry<V> bucket = table[bucket_index];
        long bitmap = bucket.getNeiRecBmp();
        long mask = 1;
        for (int i = 0; i < NEIGHBOURHOOD_SIZE; ++i, mask <<= 1) {
            if((mask & bitmap) >= 1) {
                HashEntry<V> check_bucket = table[bucket_index+i];
                if(key == check_bucket.getKey()) {
                    return check_bucket.getValue();
                }
            }
        }
        return null;
    }
}
