package cat.altimiras.shepherd.storage;

/**
 * @param <K> Key type
 * @param <V> Value input type
 * @param <S> Value output type, storage can transform it. For instance, it is added a Object and it is return a list of Objects
 */
public interface ValuesStorage<K, V, S> {

	/**
	 * Adds V to all V stored behind K
	 *
	 * @param key
	 * @param value
	 */
	void append(K key, V value);

	void remove(K key);

	S get(K key);

	void override(K key, S value);

	Iterable<K> keys();

}
