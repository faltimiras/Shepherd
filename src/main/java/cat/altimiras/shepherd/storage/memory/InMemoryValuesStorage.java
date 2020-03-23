package cat.altimiras.shepherd.storage.memory;

import cat.altimiras.shepherd.storage.ValuesStorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryValuesStorage<K, V> implements ValuesStorage<K, V, List<V>> {

	private Map<K, List<V>> storage = new HashMap<>();

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
		return Collections.unmodifiableList(storage.get(key));
	}

	@Override
	public List<V> drain(K key) {
		return storage.remove(key);
	}

	@Override
	public void override(K key, List<V> value) {
		storage.put(key, value);
	}

	@Override
	public List<V> publish(K key) {
		return storage.get(key);
	}

	Map<K, List<V>> getStorage() {
		return storage;
	}
}