package cat.altimiras.shepherd.storage;

import cat.altimiras.shepherd.Metadata;
import java.util.Iterator;

public interface MetadataStorage<K> {

	void put(K key, Metadata<K> metadata);

	void remove(K key);

	Metadata<K> get(K key);

	Iterator<Metadata<K>> values();
}
