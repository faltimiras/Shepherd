package cat.altimiras.shepherd.storage.memory;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.storage.MetadataStorage;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemoryMetadataStorage<K> implements MetadataStorage<K> {

	//order of insertion is important
	private Map<K, Metadata<K>> storage = new LinkedHashMap<>();

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
}
