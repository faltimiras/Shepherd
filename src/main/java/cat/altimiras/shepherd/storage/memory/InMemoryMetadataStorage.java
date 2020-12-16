package cat.altimiras.shepherd.storage.memory;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.storage.MetadataStorage;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InMemoryMetadataStorage<K> implements MetadataStorage<K> {

	private final Map<K, Metadata<K>> storage = new HashMap<>();

	@Override
	public void put(K key, Metadata<K> metadata) {
		storage.put(key, metadata);
	}

	@Override
	public void remove(K key) {
		storage.remove(key);
	}

	@Override
	public Metadata<K> get(K key) {
		return storage.get(key);
	}

	@Override
	public Iterator<Metadata<K>> values() {
		return storage.values().iterator();
	}

	Map<K, Metadata<K>> getStorage() {
		return storage;
	}
}
