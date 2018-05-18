package cat.altimiras.shepherd.monitoring;

import cat.altimiras.shepherd.monitoring.metric.Metric;

import java.util.Map;

public interface StatsListener {

	void push(Map<Stats, Metric> stats);

}
