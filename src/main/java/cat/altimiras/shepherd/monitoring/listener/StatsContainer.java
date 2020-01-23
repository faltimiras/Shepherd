package cat.altimiras.shepherd.monitoring.listener;

import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.metric.Metric;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatsContainer implements StatsListener {

	private List<Map<Stats, Metric>> stats;

	@Override
	public void push(List<Map<Stats, Metric>> stats) {
		this.stats = stats;
	}

	public List<Map<Stats, Metric>> getStats(){
		return stats;
	}
}
