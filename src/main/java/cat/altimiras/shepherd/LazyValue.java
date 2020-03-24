package cat.altimiras.shepherd;

import cat.altimiras.shepherd.storage.ValuesStorage;

import java.util.List;

public class LazyValue<K, V, S> {

	private ValuesStorage<K, V, S> storage;
	private K key;

	public LazyValue(ValuesStorage storage, K key) {
		this.storage = storage;
		this.key = key;
	}

	public S get() {
		return this.storage.get(key);
	}
}