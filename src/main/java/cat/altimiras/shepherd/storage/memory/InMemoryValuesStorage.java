package cat.altimiras.shepherd.storage.memory;

import cat.altimiras.shepherd.storage.ValuesStorage;
import java.util.HashMap;
import java.util.Map;

public class InMemoryValuesStorage<K, V> implements ValuesStorage<K, V, V> {

	private final Map<K, V> storage = new HashMap<>();

	@Override
	public void append(K key, V value) {
		storage.put(key, value);
	}

	@Override
	public void remove(K key) {
		storage.remove(key);
	}

	@Override
	public V get(K key) {
		return storage.get(key);
	}

	@Override
	public void override(K key, V value) {
		storage.put(key, value);
	}

	@Override
	public Iterable<K> keys() {
		return storage.keySet();
	}

	Map<K, V> getStorage() {
		return storage;
	}


}