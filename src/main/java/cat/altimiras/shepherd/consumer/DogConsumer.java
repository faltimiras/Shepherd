package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DogConsumer<K, V, S> extends QueueConsumer<K, V, S> {

	private final Scheduler scheduler;
	private final List<Rule<V>> rulesTimeout;
	private final long precision;
	private final Clock clock;
	private final RuleExecutor<V> ruleExecutorTimeout;

	public DogConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V>> rules, RuleExecutor<V> ruleExecutor, BlockingQueue<InputValue<K, V>> queue, Scheduler scheduler, List<Rule<V>> rulesTimeout, Duration precision, Clock clock, RuleExecutor<V> ruleTimeoutExecutor, Consumer<S> callback) {
		super(metadataStorage, valuesStorage, rules, queue, ruleExecutor, callback);
		this.scheduler = scheduler;
		this.rulesTimeout = rulesTimeout;
		this.precision = precision.toMillis();
		this.clock = clock;
		this.ruleExecutorTimeout = ruleTimeoutExecutor;
	}

	@Override
	public void run() {
		try {

			long lastExecutionDurationMs = 0;

			if (scheduler != null) {
				while (true) {

					long millis = this.scheduler.calculateWaitingTime(lastExecutionDurationMs);

					if (millis < 0) {
						Thread.sleep(-millis);
					} else if (millis == 0) {
						checkTimeouts(false);
					} else {
						log.info("Waiting for next element. Max ms: {}", millis);
						InputValue inputValue = queue.poll(millis, TimeUnit.MILLISECONDS);
						if (inputValue == null) {
							checkTimeouts(false);
						} else {
							long ini = clock.millis();
							consume(inputValue);
							lastExecutionDurationMs = ini - clock.millis();
						}
					}
				}
			}
		} catch (InterruptedException e) {
			//nothing to do
		} catch (Exception e) {
			log.error("Error executing shepherd", e);
			e.printStackTrace();
		}
	}

	public void checkTimeouts(boolean force) {

		log.debug("Dog gonna run for timeouts");

		Iterator<Metadata<K>> it = metadataStorage.values();

		long now = clock.millis();

		boolean stop = false;
		while (it.hasNext() && !stop) {
			Metadata<K> metadata = it.next();
			long diff = now - metadata.getCreationTs();

			if (force || (diff - precision) > 0) {
				RuleResult<V> ruleResult = ruleExecutorTimeout.execute(metadata, null, new LazyValue(valuesStorage, metadata.getKey()), rulesTimeout);

				boolean needsToRemoveMetadataForThisKey = postProcess(metadata.getKey(), null, metadata, ruleResult);
				if (needsToRemoveMetadataForThisKey) {
					it.remove();
				}

			} else {
				stop = true; //elements are ordered by creation time. Make no sense to continue if one is not older, next one wont be
			}
		}

		if (scheduler != null) {
			scheduler.justExecuted();
		}
	}
}