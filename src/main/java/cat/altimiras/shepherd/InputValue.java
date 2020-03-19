package cat.altimiras.shepherd;

import java.time.Clock;

public class InputValue<T> {

	private static final Clock clock = Clock.systemUTC();
	private final T value;
	private final Object key;
	private final long ingestionTs;

	public InputValue(T value, Object key) {
		this.value = value;
		this.key = key;
		this.ingestionTs = clock.millis();
	}

	public InputValue(T value, Object key, long ingestionTs) {
		this.value = value;
		this.key = key;
		if (ingestionTs == -1) {
			this.ingestionTs = clock.millis();
		} else {
			this.ingestionTs = ingestionTs;
		}
	}

	public T getValue() {
		return value;
	}

	public long getIngestionTs() {
		return ingestionTs;
	}

	public Object getKey() {
		return key;
	}
}
