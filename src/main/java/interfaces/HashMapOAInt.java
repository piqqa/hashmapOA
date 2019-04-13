package interfaces;

public interface HashMapOAInt<V> {
    V remove(int key);
    boolean containsKey(int key);
    boolean add(int key,V value);
    int size();
    V get(int key);
}
