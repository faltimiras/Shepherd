package cat.altimiras.shepherd.monitoring.metric;

public class Metric {

	protected long value;

	public Metric(long value) {
		this.value = value;
	}

	public long getValue() {
		return value;
	}

	long add(long toAdd) {
		this.value += toAdd;
		return this.value;
	}

	public String format() {
		return String.valueOf(value);
	}

}
