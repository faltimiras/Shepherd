package cat.altimiras.shepherd.storage;

import cat.altimiras.shepherd.Metadata;

import java.util.Iterator;

public interface MetadataStorage<K> {

	void put(K key, Metadata<K> metadata);

	void remove(K key);

	Metadata<K> get(K key);

	/**
	 * Metadata elements must be provided in Oreder by ASC creationTime
	 *
	 * @return
	 */
	Iterator<Metadata<K>> values();
}
