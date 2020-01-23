package cat.altimiras.shepherd.monitoring;

import cat.altimiras.shepherd.monitoring.metric.Metric;

import java.util.List;
import java.util.Map;

public interface StatsListener {

	void push(List<Map<Stats, Metric>> stats);

}
