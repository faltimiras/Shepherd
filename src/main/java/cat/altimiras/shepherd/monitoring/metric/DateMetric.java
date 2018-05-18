package cat.altimiras.shepherd.monitoring.metric;

import java.util.Date;

public class DateMetric extends Metric {

	private final boolean oldest;

	public DateMetric(long value, boolean oldest) {
		super(value);
		this.oldest = oldest;
	}

	public DateMetric(long value) {
		super(value);
		this.oldest = true;
	}

	boolean isOldest() {
		return oldest;
	}

	@Override
	public String format() {
		if (value == 0) {
			return "-";
		}
		return new Date(value).toString();
	}
}
