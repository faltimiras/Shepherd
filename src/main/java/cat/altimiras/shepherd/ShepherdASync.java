package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.WindowedConsumer;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShepherdASync<K, V, S> extends ShepherdBase<K, V, S> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdASync.class);

	private final LinkedBlockingQueue<InputValue<K, V>>[] queues;

	private final ExecutorService pool;

	private final int threads;

	ShepherdASync(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, int thread, Function keyExtractor, List<Rule<V, S>> rules, RuleExecutor<V, S> ruleExecutor, Consumer<S> callback, Window window, Metrics metrics, Clock clock) {

		super(keyExtractor, callback, ruleExecutor, thread, window, metrics, clock);

		this.threads = thread;
		this.queues = new LinkedBlockingQueue[thread];
		this.pool = Executors.newFixedThreadPool(thread);

		initializeQueues();
		startConsumers(metadataStorageProvider, valuesStorageProvider, rules, window);
	}

	private void initializeQueues() {
		for (int i = 0; i < threads; i++) {
			queues[i] = new LinkedBlockingQueue();
		}
	}

	private void startConsumers(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, List<Rule<V, S>> rules, Window window) {
		for (int i = 0; i < threads; i++) {
			QueueConsumer qc = getAsyncConsumer(metadataStorageProvider, valuesStorageProvider, rules, window, i);
			consumers.add(qc);
			pool.submit(qc);
		}
	}

	private QueueConsumer getAsyncConsumer(Supplier<MetadataStorage> metadataStorageProvider, Supplier<ValuesStorage> valuesStorageProvider, List<Rule<V, S>> rules, Window window, int index) {
		if (window != null) {
			return new WindowedConsumer(
					metadataStorageProvider.get(),
					valuesStorageProvider.get(),
					rules,
					this.ruleExecutor,
					this.queues[index],
					(Scheduler) window.getSchedulerProvider().get(),
					window.getRule(),
					this.callback,
					metrics,
					clock);
		} else {
			return new BasicConsumer(
					metadataStorageProvider.get(),
					valuesStorageProvider.get(),
					rules,
					this.queues[index],
					this.ruleExecutor,
					this.callback,
					metrics,
					clock);
		}
	}

	public boolean areQueuesEmpty() {

		for (LinkedBlockingQueue<InputValue<K, V>> queue : queues) {
			if (!queue.isEmpty()) {
				return false;
			}
		}
		return true;
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

				if (queues.length == 1) {
					queues[0].put(inputValue);
				} else {
					int indexQueue = key.hashCode() % threads;
					//hashcode can be negative
					queues[indexQueue < 0 ? -indexQueue : indexQueue].put(inputValue);
				}
				metrics.pendingInc();
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
	public boolean add(V v) {
		return add(v, clock.millis());
	}

	@Override
	public void stop(boolean forceTimeout) {
		if (forceTimeout) {
			this.checkWindows();
		}
		pool.shutdown();
	}
}