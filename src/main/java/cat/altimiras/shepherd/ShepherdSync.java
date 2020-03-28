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

	ShepherdSync(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, Function keyExtractor, List<Rule<V, S>> rules, RuleExecutor<V, S> ruleExecutor, Consumer<S> callback, Window window, Metrics metrics, Clock clock) {

		super(keyExtractor, callback, ruleExecutor, 1, window, metrics, clock);

		if (window != null) {
			this.consumer = new WindowedConsumer(metadataStorageProvider.get(), valuesStorageProvider.get(), rules, this.ruleExecutor, null, null, window.getRule(), this.callback, metrics);
		} else {
			this.consumer = new BasicConsumer(metadataStorageProvider.get(), valuesStorageProvider.get(), rules, null, this.ruleExecutor, this.callback, metrics);
		}

		this.consumers.add(consumer);
	}

	@Override
	public boolean add(K key, V v, long timestamp) {
		try {
			if (key == null) {
				log.error("Extracted key == null, discarding object");
				log.info("Element discarded {}", v);
				return false;
			} else {
				InputValue inputValue;
				if (isWindowed) {
					//timestamp is part of the key, timestamp to expire the key is the after now + duration
					inputValue = new InputValue(v, window.getRule().adaptKey(key, timestamp), timestamp);
				} else {
					inputValue = new InputValue(v, key, timestamp);
				}

				this.consumer.consume(inputValue);
				this.metrics.pendingInc();
			}
			return true;
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(K key, V v) {
		return add(key, v, clock.millis());
	}

	@Override
	public boolean add(V v, long timestamp) {
		try {
			K key = keyExtractor.apply(v);
			return add(key, v, timestamp);
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(V v) {
		return add(v, clock.millis());
	}

	@Override
	public void stop(boolean closeWindows) {
		if (closeWindows) {
			this.checkWindows();
		}
	}
}