package cat.altimiras.shepherd;

import java.util.HashMap;
import java.util.Map;

public class Metadata<K> {

	private final K key;
	private final long creationTs;
	private Map<String, Object> metadata;
	private long lastElementTs;
	private long elementsCount;

	public Metadata(K key, long creationTs) {
		this.key = key;
		this.creationTs = creationTs;
	}

	public K getKey() {
		return key;
	}

	public long getCreationTs() {
		return creationTs;
	}

	public long getLastElementTs() {
		return lastElementTs;
	}

	public void setLastElementTs(long lastElementTs) {
		this.lastElementTs = lastElementTs;
	}

	public void add(String key, Object value) {
		if (this.metadata == null) {
			this.metadata = new HashMap<>(2);
		}
		this.metadata.put(key, value);
	}

	public void remove(String key) {
		if (this.metadata != null) {
			this.metadata.remove(key);
		}
	}

	public Object get(String key) {
		if (this.metadata == null) {
			return null;
		} else {
			return this.metadata.get(key);
		}
	}

	public long getElementsCount() {
		return elementsCount;
	}

	public void setElementsCount(long elementsCount) {
		this.elementsCount = elementsCount;
	}

	public void incElementsCount() {
		this.elementsCount++;
	}

	public void decElementsCount() {
		this.elementsCount--;
	}

	public void incElementsCount(long value) {
		this.elementsCount = this.elementsCount + value;
	}

	public void decElementsCount(long value) {
		this.elementsCount = this.elementsCount - value;
	}
}