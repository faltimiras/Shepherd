package cat.altimiras.shepherd.storage.memory;

import cat.altimiras.shepherd.storage.ValuesStorage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryListValuesStorage<K, V> implements ValuesStorage<K, V, List<V>> {

	private final Map<K, List<V>> storage = new HashMap<>();

	@Override
	public void append(K key, V value) {
		List<V> list = storage.get(key);
		if (list == null) {
			list = new ArrayList<>();
			storage.put(key, list);
		}
		list.add(value);
	}

	@Override
	public void remove(K key) {
		storage.remove(key);
	}

	@Override
	public List<V> get(K key) {
		List<V> values = storage.get(key);
		return values == null ? null : Collections.unmodifiableList(storage.get(key));
	}

	@Override
	public void override(K key, List<V> value) {
		storage.put(key, value);
	}

	@Override
	public Iterable<K> keys() {
		return storage.keySet();
	}

	Map<K, List<V>> getStorage() {
		return storage;
	}
}