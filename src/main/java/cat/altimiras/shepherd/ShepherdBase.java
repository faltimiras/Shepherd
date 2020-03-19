package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.DogConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"unchecked", "rawtypes"})
abstract class ShepherdBase<K,V, S> implements Shepherd<K,V> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdBase.class);

	protected final Function<Object, K> keyExtractor;

	protected final Consumer<S> callback;

	protected final RuleExecutor<V> ruleExecutor;

	//private final ScheduledExecutorService statPool;

	//private final List<StatsListener> statsListeners;

	private final boolean hasDog;

	protected final List<QueueConsumer> consumers;


	public ShepherdBase(Function keyExtractor, Consumer<S> callback, RuleExecutor<V> ruleExecutor, int numConsumers, boolean hasDog, Optional<ShepherdBuilder.Monitoring> monitoring) {
		this.keyExtractor = keyExtractor;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.consumers = new ArrayList<>(numConsumers);
		this.hasDog = hasDog;
/*
		if (monitoring.isPresent()) {
			/*this.statsListeners = monitoring.get().getStatsListeners();
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
		*/
	}


	public void forceTimeout() {
		forceTimeout(false);
	}

	public void forceTimeout(boolean force) {
		if (hasDog) {
			consumers.forEach((c -> ((DogConsumer) c).checkTimeouts(force)));
		} else {
			log.info("Ignoring force timeouts. As no dog is configured");
		}
	}
}
