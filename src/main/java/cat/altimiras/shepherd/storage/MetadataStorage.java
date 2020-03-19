package cat.altimiras.shepherd.storage;

import cat.altimiras.shepherd.Metadata;

import java.util.Iterator;

public interface MetadataStorage<K> {

	void put(K key, Metadata metadata);

	void remove(K key);

	Metadata get(K key);

	/**
	 * Metadata elements must be provided in Oreder by ASC creationTime
	 * @return
	 */
	Iterator<Metadata> values();
}
