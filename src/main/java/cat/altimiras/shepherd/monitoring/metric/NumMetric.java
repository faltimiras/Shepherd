package cat.altimiras.shepherd.monitoring.metric;

public class NumMetric implements Metric<Long> {

	private long value;

	public NumMetric(long value) {
		this.value = value;
	}

	@Override
	public Metric<Long> merge(Metric<Long> otherMetric) {
		if (otherMetric != null){
			this.value += otherMetric.get();
		}
		return this;
	}

	@Override
	public Long get() {
		return value;
	}
}
