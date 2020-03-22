package cat.altimiras.shepherd.storage;

import cat.altimiras.shepherd.Metadata;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 * @param <K> Key type
 * @param <V> Value input type
 * @param <S> Value output type, storage can transform it. For instance, it is added a Object and it is return a list of Objects
 */
public interface ValuesStorage<K, V, S> {

	/**
	 * Adds V to all V stored behind K
	 * @param key
	 * @param value
	 */
	void append(K key, V value);

	void remove(K key);

	List<V> get(K key);

	/**
	 * Get and remove all values behind S
	 * @param key
	 * @return
	 */
	S drain(K key);

	void override(K key, List<V> value);

	S publish(K key);

}
