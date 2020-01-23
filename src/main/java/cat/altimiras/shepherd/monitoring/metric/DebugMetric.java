package cat.altimiras.shepherd.monitoring.metric;

import java.util.List;
import java.util.Map;

public class DebugMetric implements Metric<Map<String, List<Map<String, String>>>> {

	private Map<String, List<Map<String, String>>> value;

	public DebugMetric(Map<String, List<Map<String, String>>> value) {
		this.value = value;
	}

	@Override
	public Metric<Map<String, List<Map<String, String>>>> merge(Metric<Map<String, List<Map<String, String>>>> otherMetric) {
		if (otherMetric != null) {
			this.value.putAll(otherMetric.get());
		}
		return this;
	}

	@Override
	public Map<String, List<Map<String, String>>> get() {
		return value;
	}
}
