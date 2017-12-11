package cat.altimiras.shepherd;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Element<T> {

	private final Clock clock = Clock.systemUTC();

	private Object key;
	private List<T> values;
	private Instant creationTs;
	private Instant lastElementTs;

	public Element(Object key, List<T> values, long timestamp) {
		this.key = key;
		this.values = values;
		this.creationTs = Instant.ofEpochMilli(timestamp);
		this.lastElementTs = Instant.ofEpochMilli(timestamp);
		;
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

	void addValue(T value) {
		this.values.add(value);
		this.lastElementTs = clock.instant();
	}

	public List<T> getValues() {
		return Collections.unmodifiableList(values);
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
}
