package cat.altimiras.shepherd;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Element<T> {

	private final Clock clock = Clock.systemUTC();

	private Object key;
	private List<T> values;
	private Instant creationTs;
	private Instant lastElementTs;
	private Map<String, Object> metadata;

	public Element(Object key, List<T> values, long timestamp) {
		this.key = key;
		this.values = values;
		this.creationTs = Instant.ofEpochMilli(timestamp);
		this.lastElementTs = Instant.ofEpochMilli(timestamp);
	}

	public Element(Object key, List<T> values, Instant timestamp) {
		this.key = key;
		this.values = values;
		this.creationTs = timestamp;
		this.lastElementTs = timestamp;
	}

	public Element(Object key, List<T> values) {
		this.key = key;
		this.values = values;
		this.creationTs = clock.instant();
		this.lastElementTs = clock.instant();
	}

	public Element(Object key) {
		this.key = key;
		this.values = new ArrayList<>();
		this.creationTs = clock.instant();
	}

	public Element(Object key, T value) {
		this.key = key;
		this.values = new ArrayList<T>();
		this.values.add(value);
		this.creationTs = clock.instant();
		this.lastElementTs = clock.instant();
	}

	public Element(Object key, T value, long timestamp) {
		this.key = key;
		this.values = new ArrayList<T>();
		this.values.add(value);
		this.creationTs = Instant.ofEpochMilli(timestamp);
		this.lastElementTs = Instant.ofEpochMilli(timestamp);
	}

	public Element(Object key, T value, Instant timestamp) {
		this.key = key;
		this.values = new ArrayList<T>();
		this.values.add(value);
		this.creationTs = timestamp;
		this.lastElementTs = timestamp;
	}

	public Element(Element origin, int numberOfElementGroup, boolean discard, boolean keepMetadata) {
		this.key = origin.key;
		this.creationTs = origin.creationTs;
		this.lastElementTs = origin.lastElementTs;

		if (keepMetadata) {
			this.metadata = origin.metadata;
		}

		int size = origin.getValues().size();

		if (numberOfElementGroup >= 0) {
			if (discard) {
				int sublistSize = size - (numberOfElementGroup + 1);
				this.values = sublistSize > 0 ? origin.values.subList(numberOfElementGroup + 1, size) : null;
			}
			else {
				this.values = numberOfElementGroup == 0 ? null : origin.values.subList(0, numberOfElementGroup);
			}
		}
		else {
			if (discard) {
				int end = size + numberOfElementGroup;
				this.values = end > 0 ? origin.values.subList(0, size + numberOfElementGroup) : null;
			}
			else {
				int sublistSize = size - (size + numberOfElementGroup);
				this.values = sublistSize > 0 ? origin.values.subList(size + numberOfElementGroup, size) : null;
			}
		}
	}

	public Element(Element origin, int numberOfElementGroup, boolean discard) {
		this(origin, numberOfElementGroup, discard, true);
	}

	void addValue(T value) {
		this.values.add(value);
		this.lastElementTs = clock.instant();
	}

	public void addMetadata(String key, Object value) {
		if (metadata == null) {
			metadata = new HashMap<>();
		}
		metadata.put(key, value);
	}

	public Object getMetadata(String key) {
		return metadata == null ? null : metadata.get(key);
	}

	public List<T> getValues() {
		return values == null ? null : Collections.unmodifiableList(values);
	}

	public Instant getCreationTs() {
		return creationTs;
	}

	public Instant getLastElementTs() {
		return lastElementTs;
	}

	public Object getKey() {
		return key;
	}

	public T getLastValue() {
		if (this.values.isEmpty()) {
			return null;
		}
		return this.values.get(this.values.size() - 1);
	}

	public T getFirstValue() {
		if (this.values.isEmpty()) {
			return null;
		}
		return this.values.get(0);
	}

	public List<T> getValues(int index) {
		if (index >= 0) {
			return getValues().subList(0, index);
		}
		else {
			return getValues().subList(values.size() + index, values.size());
		}
	}

	public List<T> getValuesDiscarding(int index) {
		if (index >= 0) {
			return getValues().subList(index, values.size());
		}
		else {
			return getValues().subList(0, values.size() + index);
		}
	}


}
