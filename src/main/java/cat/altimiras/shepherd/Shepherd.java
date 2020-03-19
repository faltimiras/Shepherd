package cat.altimiras.shepherd;

public interface Shepherd<T> {

	boolean add(T t, long timestamp);

	boolean add(T t);

	boolean add(Object key, T t, long timestamp);

	boolean add(Object key, T t);

	void forceTimeout();

	void forceTimeout(boolean force);

	void stop(boolean forceTimeout);

}
