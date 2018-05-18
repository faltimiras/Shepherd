package cat.altimiras.shepherd.monitoring.listener;

import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.metric.Metric;

import java.util.Date;
import java.util.Map;

public class ConsoleStatsListener implements StatsListener {

	@Override
	public void push(Map<Stats, Metric> stats) {

		StringBuffer buffer = new StringBuffer(new Date().toString());
		buffer.append(System.lineSeparator());
		for (Map.Entry<Stats, Metric> e : stats.entrySet()) {
			buffer.append(e.getKey().name());
			buffer.append(":");
			buffer.append(e.getValue().format());
			buffer.append(System.lineSeparator());
		}
		System.out.println("Shepherd stats:" + buffer);
	}
}