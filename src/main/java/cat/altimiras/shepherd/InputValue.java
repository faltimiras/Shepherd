package cat.altimiras.shepherd;

public class InputValue<K, T> {

	private final T value;

	private final K key;

	private final long timestamp;

	public InputValue(T value, K key, long timestamp) {
		this.value = value;
		this.key = key;
		this.timestamp = timestamp;
	}

	public T getValue() {
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public K getKey() {
		return key;
	}

	@Override
	public String toString() {
		return "InputValue{" +
				"value=" + value +
				", key=" + key +
				", timestamp=" + timestamp +
				'}';
	}
}