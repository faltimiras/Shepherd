package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.WindowedConsumer;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShepherdSync<K, V, S> extends ShepherdBase<K, V, S> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdSync.class);

	private final QueueConsumer<K, V, S> consumer;

	ShepherdSync(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, Function keyExtractor, List<Rule<S>> rules, RuleExecutor<V> ruleExecutor, Consumer<S> callback, Window window, Metrics metrics, Clock clock) {

		super(keyExtractor, callback, ruleExecutor, 1, window != null, metrics, clock);

		if (window != null) {
			this.consumer = new WindowedConsumer(metadataStorageProvider.get(), valuesStorageProvider.get(), rules, this.ruleExecutor, null, null, window.getRule(), window.getPrecision(), clock, this.callback, metrics);
		} else {
			this.consumer = new BasicConsumer(metadataStorageProvider.get(), valuesStorageProvider.get(), rules, null, this.ruleExecutor, this.callback, metrics);
		}

		this.consumers.add(consumer);
	}

	@Override
	public boolean add(K key, V t, long timestamp) {
		try {
			if (key == null) {
				log.error("Extracted key == null, discarding object");
				log.info("Element discarded {}", t);
				return false;
			} else {
				this.consumer.consume(new InputValue(t, key, timestamp));
				metrics.pendingInc();
			}
			return true;
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(K key, V t) {
		return add(key, t, -1);
	}

	@Override
	public boolean add(V t, long timestamp) {
		try {
			K key = keyExtractor.apply(t);
			return add(key, t, timestamp);
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(V t) {
		return add(t, System.currentTimeMillis());
	}

	@Override
	public void stop(boolean forceTimeout) {
		if (forceTimeout) {
			this.forceTimeout();
		}
	}
}