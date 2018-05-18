package cat.altimiras.shepherd.monitoring.listener;

import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.metric.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogStatsListener implements StatsListener {

	protected static Logger log = LoggerFactory.getLogger(LogStatsListener.class);

	public LogStatsListener() {
	}

	public LogStatsListener(Logger log) {
		LogStatsListener.log = log;
	}

	@Override
	public void push(Map<Stats, Metric> stats) {

		StringBuffer buffer = new StringBuffer();
		for (Map.Entry<Stats, Metric> e : stats.entrySet()) {
			buffer.append(e.getKey().name());
			buffer.append(":");
			buffer.append(e.getValue().format());
			buffer.append(System.lineSeparator());
		}
		log.info("Shepherd stats: {}", buffer);
	}
}