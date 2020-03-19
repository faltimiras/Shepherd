package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.DogConsumer;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ShepherdASync<T, S> extends ShepherdBase<T, S> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdASync.class);

	private final LinkedBlockingQueue<InputValue<T>>[] queues;

	private final ExecutorService pool;

	private final int threads;

	ShepherdASync(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, int thread, Function keyExtractor, List<Rule<S>> rules, RuleExecutor<T> ruleExecutor, Consumer<S> callback, Optional<ShepherdBuilder.Dog> dog, Optional<ShepherdBuilder.Monitoring> monitoring) {

		super(keyExtractor, callback, ruleExecutor, thread, dog.isPresent(), monitoring);

		this.threads = thread;
		this.queues = new LinkedBlockingQueue[thread];
		this.pool = Executors.newFixedThreadPool(thread);

		initializeQueues();
		startConsumers(metadataStorageProvider, valuesStorageProvider, rules, dog);
	}

	private void initializeQueues() {
		for (int i = 0; i < threads; i++) {
			queues[i] = new LinkedBlockingQueue();
		}
	}

	private void startConsumers(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, List<Rule<S>> rules, Optional<ShepherdBuilder.Dog> dog) {
		for (int i = 0; i < threads; i++) {
			QueueConsumer qc = getAsyncConsumer(metadataStorageProvider, valuesStorageProvider, rules, dog, i);
			consumers.add(qc);
			pool.submit(qc);
		}
	}

	private QueueConsumer getAsyncConsumer(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, List<Rule<S>> rules, Optional<ShepherdBuilder.Dog> dog, int index) {
		if (dog.isPresent()) {

			return new DogConsumer(
					metadataStorageProvider.get(),
					valuesStorageProvider.get(),
					rules,
					this.ruleExecutor,
					this.queues[index],
					(Scheduler) dog.get().getSchedulerProvider().get(),
					dog.get().getRulesTimeout(),
					dog.get().getPrecision(),
					Clock.systemUTC(),
					dog.get().getRuleExecutor(),
					this.callback);
		} else {
			return new BasicConsumer(
					metadataStorageProvider.get(),
					valuesStorageProvider.get(),
					rules,
					this.queues[index],
					this.ruleExecutor,
					this.callback);
		}
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
	public boolean add(Object key, T t, long timestamp) {
		try {
			if (key == null) {
				log.error("Extracted key == null, discarding object");
				log.info("Element discarded {}", t);
				return false;
			} else {
				InputValue inputValue = new InputValue(t, key, timestamp);
				if (queues.length == 1) {
					queues[0].put(inputValue);
				} else {
					int indexQueue = key.hashCode() % threads;
					//hashcode can be negative
					queues[indexQueue < 0 ? -indexQueue : indexQueue].put(inputValue);
				}
			}
			return true;
		} catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	@Override
	public boolean add(Object key, T t) {
		return add(key, t, -1);
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
		pool.shutdown();
	}

	public boolean areQueuesEmpty() {

		for (int i = 0; i < queues.length; i++) {
			if (!queues[i].isEmpty()) {
				return false;
			}
		}
		return true;
	}

}
