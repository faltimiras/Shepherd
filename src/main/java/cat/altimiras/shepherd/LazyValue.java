package cat.altimiras.shepherd;

import cat.altimiras.shepherd.storage.ValuesStorage;

import java.util.List;

public class LazyValue<K, V> {

	private ValuesStorage<K,V, ?> storage;
	private K key;

	public LazyValue(ValuesStorage storage, K key) {
		this.storage = storage;
		this.key = key;
	}

	public List<V> get() {
		return this.storage.get(key);
	}
}
