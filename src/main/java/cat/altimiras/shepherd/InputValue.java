package cat.altimiras.shepherd;

import java.time.Clock;

public class InputValue<K, T> {

	final private static Clock clock = Clock.systemUTC();
	private final T value;
	private final K key;
	private final long ingestionTs;

	public InputValue(T value, K key) {
		this.value = value;
		this.key = key;
		this.ingestionTs = clock.millis();
	}

	public InputValue(T value, K key, long ingestionTs) {
		this.value = value;
		this.key = key;
		this.ingestionTs = ingestionTs;
	}

	public T getValue() {
		return value;
	}

	public long getIngestionTs() {
		return ingestionTs;
	}

	public K getKey() {
		return key;
	}
}