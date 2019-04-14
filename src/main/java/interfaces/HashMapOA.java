package interfaces;

public interface HashMapOA<V> {
    V remove(int key);
    boolean containsKey(int key);
    V put(int key,V value);
    int size();
    V get(int key);
}
