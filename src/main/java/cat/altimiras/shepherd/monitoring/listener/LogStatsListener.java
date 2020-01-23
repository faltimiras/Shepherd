package cat.altimiras.shepherd.monitoring.listener;

import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.metric.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class LogStatsListener implements StatsListener {

	protected Logger log;

	public LogStatsListener() {
		this.log = LoggerFactory.getLogger(LogStatsListener.class);
	}

	public LogStatsListener(Class loggerClass) {
		this.log = LoggerFactory.getLogger(loggerClass);
	}

	@Override
	public void push(List<Map<Stats, Metric>> stats) {

		StringBuffer buffer = new StringBuffer();
		for (Map<Stats, Metric> partitionMetrics : stats) {
			for (Map.Entry<Stats, Metric> e : partitionMetrics.entrySet()) {
				buffer.append(e.getKey().name());
				buffer.append(":");
				buffer.append(e.getValue());
				buffer.append(System.lineSeparator());
			}
		}
		log.info("Shepherd stats: {}", buffer);
	}
}