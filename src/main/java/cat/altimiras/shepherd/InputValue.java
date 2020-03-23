package cat.altimiras.shepherd;

public class InputValue<K, T> {

	private final T value;
	private final K key;
	private final long ingestionTs;

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