package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.DogConsumer;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ShepherdSync<T, S> extends ShepherdBase<T, S> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdSync.class);

	private final QueueConsumer<T, S> consumer;

	ShepherdSync(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, Function keyExtractor, List<Rule<S>> rules, RuleExecutor<T> ruleExecutor, Consumer<S> callback, Optional<ShepherdBuilder.Dog> dog, Optional<ShepherdBuilder.Monitoring> monitoring) {

		super(keyExtractor, callback, ruleExecutor, 1, dog.isPresent(), monitoring);

		if (dog.isPresent()) {
			this.consumer = new DogConsumer(metadataStorageProvider.get(), valuesStorageProvider.get(), rules, this.ruleExecutor, null, null, dog.get().getRulesTimeout(), dog.get().getPrecision(), Clock.systemUTC(), dog.get().getRuleExecutor(), this.callback);
		} else {
			this.consumer = new BasicConsumer(metadataStorageProvider.get(), valuesStorageProvider.get(), rules, null, this.ruleExecutor, this.callback);
		}

		this.consumers.add(consumer);

	}

	@Override
	public boolean add(Object key, T t, long timestamp) {
		try {
			if (key == null) {
				log.error("Extracted key == null, discarding object");
				log.info("Element discarded {0}", t);
				return false;
			} else {
				this.consumer.consume(new InputValue(t, key, timestamp));
			}
			return true;
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(Object key, T t) {
		return add(key,t, -1);
	}

	@Override
	public boolean add(T t, long timestamp) {
		try {
			Object key = keyExtractor.apply(t);
			return add(key, t, timestamp);
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(T t) {
		return add(t, -1);
	}

	@Override
	public void stop(boolean forceTimeout) {
		if (forceTimeout) {
			this.forceTimeout(true);
		}
	}
}
