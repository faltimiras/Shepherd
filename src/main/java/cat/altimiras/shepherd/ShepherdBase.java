package cat.altimiras.shepherd;

import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.metric.Metric;
import cat.altimiras.shepherd.monitoring.metric.MetricMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class ShepherdBase<T> implements Shepherd<T> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdBase.class);

	protected final KeyExtractor keyExtractor;

	protected final Callback<T> callback;

	protected final RuleExecutor<T> ruleExecutor;

	private final ScheduledExecutorService statPool;

	private final List<StatsListener> statsListeners;

	protected final List<QueueConsumer> consumers;

	public ShepherdBase(KeyExtractor keyExtractor, Callback<T> callback, RuleExecutor<T> ruleExecutor, int numConsumers, Optional<ShepherdBuilder.Monitoring> monitoring) {
		this.keyExtractor = keyExtractor;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.consumers = new ArrayList<>(numConsumers);

		if (monitoring.isPresent()) {
			this.statsListeners = monitoring.get().getStatsListeners();
			this.statPool = Executors.newScheduledThreadPool(1);
			this.statPool.scheduleWithFixedDelay(
					() -> collectStatsConsumers(monitoring.get().getLevel()),
					monitoring.get().getEvery().toMillis(),
					monitoring.get().getEvery().toMillis(),
					TimeUnit.MILLISECONDS
			);
		}
		else {
			this.statsListeners = null;
			this.statPool = null;
		}
	}

	private void collectStatsConsumers(Level level) {
		try {
			log.debug("Collection stats loop just started");
			Map<Stats, Metric> aggregated = new HashMap<>(Stats.values().length);

			for (QueueConsumer consumer : consumers) {
				Map<Stats, Metric> statsConsumer = consumer.getStats(level);
				MetricMerger.merge(aggregated, statsConsumer);
			}

			for (StatsListener listener : statsListeners) {
				listener.push(aggregated);
			}
		}
		catch (Exception e) {
			log.error("Error collecting shepherd stats", e);
		}
	}

}
