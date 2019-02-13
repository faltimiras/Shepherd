package cat.altimiras.shepherd;

import java.time.Instant;

public interface Shepherd<T> {

	boolean add(T t, Instant timestmap);

	boolean add(T t, Long timestmap);

	boolean add(T t);

	void forceTimeout();

	void forceTimeout(boolean force);

}
