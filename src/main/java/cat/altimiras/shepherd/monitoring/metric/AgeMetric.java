package cat.altimiras.shepherd.monitoring.metric;

public class AgeMetric implements Metric<Long> {

	private long value;

	private boolean oldest;

	public AgeMetric(long value, boolean oldest) {
		this.value = value;
		this.oldest = oldest;
	}

	public AgeMetric(long value) {
		this.value = value;
		this.oldest = true;
	}

	@Override
	public Metric<Long> merge(Metric<Long> otherMetric) {
		if (otherMetric != null) {
			if (this.oldest) {
				this.value = this.value > otherMetric.get().longValue() ? otherMetric.get().longValue() : this.value;
			} else {
				this.value = this.value > otherMetric.get().longValue() ? this.value : otherMetric.get().longValue();
			}
		}
		return this;
	}

	@Override
	public Long get() {
		return value;
	}
}
