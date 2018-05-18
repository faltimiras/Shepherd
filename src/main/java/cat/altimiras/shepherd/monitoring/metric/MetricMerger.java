package cat.altimiras.shepherd.monitoring.metric;

import cat.altimiras.shepherd.monitoring.Stats;

import java.util.Map;

public class MetricMerger {

	public static synchronized void merge(Map<Stats, Metric> aggregated, Map<Stats, Metric> stats) {
		for (Map.Entry<Stats, Metric> e : stats.entrySet()) {
			Metric value = aggregated.get(e.getKey());
			if (value == null) {
				aggregated.put(e.getKey(), e.getValue());
			}
			else {
				if (value instanceof  DateMetric){ //date metrics are not aggregated, keeps old or newer according config
					if(((DateMetric)value).isOldest()){
						aggregated.put(e.getKey(), value.getValue() > e.getValue().getValue() ? e.getValue() : value);
					}
					else {
						aggregated.put(e.getKey(), value.getValue() > e.getValue().getValue() ? value : e.getValue());
					}
				}
				else {
					value.add(e.getValue().getValue());
					aggregated.put(e.getKey(), value);
				}
			}
		}
	}
}
