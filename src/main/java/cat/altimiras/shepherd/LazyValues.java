package cat.altimiras.shepherd;

import cat.altimiras.shepherd.storage.ValuesStorage;

public class LazyValues<K, V, S> {

	private ValuesStorage<K, V, S> storage;
	private K key;

	public LazyValues(ValuesStorage storage, K key) {
		this.storage = storage;
		this.key = key;
	}

	public S get() {
		return this.storage.get(key);
	}
}