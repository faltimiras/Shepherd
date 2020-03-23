package cat.altimiras.shepherd;

public interface Shepherd<K, V> {

	boolean add(V t, long timestamp);

	boolean add(V t);

	boolean add(K key, V t, long timestamp);

	boolean add(K key, V t);

	void forceTimeout();

	void stop(boolean forceTimeout);
}