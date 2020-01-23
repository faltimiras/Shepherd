package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.DogConsumer;
import cat.altimiras.shepherd.monitoring.debug.ElementDebugSerializer;
import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.metric.Metric;
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

	private final boolean hasDog;

	protected final List<QueueConsumer> consumers;


	public ShepherdBase(KeyExtractor keyExtractor, Callback<T> callback, RuleExecutor<T> ruleExecutor, int numConsumers, boolean hasDog, Optional<ShepherdBuilder.Monitoring> monitoring) {
		this.keyExtractor = keyExtractor;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.consumers = new ArrayList<>(numConsumers);
		this.hasDog = hasDog;

		if (monitoring.isPresent()) {
			this.statsListeners = monitoring.get().getStatsListeners();
			this.statPool = Executors.newScheduledThreadPool(1);
			this.statPool.scheduleWithFixedDelay(
					() -> collectStatsConsumers(monitoring.get().getLevel(), monitoring.get().getElementDebugSerializer()),
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

	private void collectStatsConsumers(Level level, ElementDebugSerializer debugSerializer) {
		try {
			log.debug("Collection stats loop just started");
			List<Map<Stats, Metric>> metrics = new ArrayList<>(consumers.size());

			for (QueueConsumer consumer : consumers) {
				metrics.add(consumer.getStats(level, debugSerializer));
			}

			if (!metrics.isEmpty()) {
				for (StatsListener listener : statsListeners) {
					listener.push(metrics);
				}
			}
		}
		catch (Exception e) {
			log.error("Error collecting shepherd stats", e);
		}
	}

	public void forceTimeout() {
		forceTimeout(false);
	}

	public void forceTimeout(boolean force) {
		if (hasDog) {
			consumers.stream().forEach((c -> ((DogConsumer)c).checkTimeouts(force)));
		}
		else {
			log.info("Ignoring force timeouts. As no dog is configured");
		}
	}
}
