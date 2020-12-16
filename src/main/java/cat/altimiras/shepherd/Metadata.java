package cat.altimiras.shepherd;

import java.time.Clock;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metadata<K> {

	protected static Logger log = LoggerFactory.getLogger(Metadata.class);

	private final K key;

	//element times
	private final long firstElementTs;

	//ingestion times
	private final long firstElementLiveTs;

	private final Clock clock;

	private Map<String, Object> metadata;

	private long elementsCount;

	private long lastElementTs;

	private long lastElementLiveTs;

	public Metadata(K key, long eventTs, Clock clock) {
		this.key = key;
		this.firstElementTs = eventTs;
		this.firstElementLiveTs = clock.millis();
		this.clock = clock;
		if (log.isDebugEnabled()) {
			log.debug("New key: {} created at {}", key, new Date(this.firstElementLiveTs));
		}
	}

	public K getKey() {
		return key;
	}

	public long getFirstElementTs() {
		return firstElementTs;
	}

	public long getLastElementTs() {
		return lastElementTs;
	}

	public void setLastElementTs(long lastElementTs) {
		this.lastElementTs = lastElementTs;
		this.lastElementLiveTs = clock.millis();
		if (log.isDebugEnabled()) {
			log.debug("Set lastElement for key: {} at {}", key, new Date(this.lastElementLiveTs));
		}
	}

	public long getFirstElementLiveTs() {
		return firstElementLiveTs;
	}

	public long getLastElementLiveTs() {
		return lastElementLiveTs;
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

	public void removeAll() {
		if (this.metadata != null) {
			this.metadata.clear();
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

	public void resetElementsCount() {
		this.elementsCount = 0;
	}

	@Override
	public String toString() {
		return "Metadata{" +
				"key=" + key +
				", firstElementTs=" + firstElementTs +
				", metadata=" + metadata +
				", lastElementTs=" + lastElementTs +
				", elementsCount=" + elementsCount +
				", firstElementLiveTs=" + firstElementLiveTs +
				", lastElementLiveTs=" + lastElementLiveTs +
				'}';
	}
}