package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.WindowedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"unchecked", "rawtypes"})
abstract class ShepherdBase<K, V, S> implements Shepherd<K, V> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdBase.class);

	protected final Metrics metrics;
	protected final Function<Object, K> keyExtractor;
	protected final Consumer<S> callback;
	protected final RuleExecutor<V, S> ruleExecutor;
	protected final Window window;
	protected final boolean isWindowed;
	protected final Clock clock;

	protected final List<QueueConsumer> consumers;

	public ShepherdBase(Function keyExtractor, Consumer<S> callback, RuleExecutor<V, S> ruleExecutor, int numConsumers, Window window, Metrics metrics, Clock clock) {
		this.keyExtractor = keyExtractor;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.consumers = new ArrayList<>(numConsumers);
		this.isWindowed = window != null;
		this.window = window;
		this.metrics = metrics;
		this.clock = clock;
	}

	public void checkWindows() {
		if (isWindowed) {
			consumers.forEach((c -> ((WindowedConsumer) c).checkWindows()));
		} else {
			log.info("Ignoring force timeouts. As Window is not configured");
		}
	}
}