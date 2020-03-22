package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Metrics;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.RuleWindow;
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

public class WindowedConsumer<K, V, S> extends QueueConsumer<K, V, S> {

	private final Scheduler scheduler;
	private final RuleWindow rulesWindow;
	private final long precision;
	private final Clock clock;

	public WindowedConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V>> rules, RuleExecutor<V> ruleExecutor, BlockingQueue<InputValue<K, V>> queue, Scheduler scheduler, RuleWindow ruleWindow, Duration precision, Clock clock, Consumer<S> callback, Metrics metrics) {
		super(metadataStorage, valuesStorage, rules, queue, ruleExecutor, callback, metrics);
		this.scheduler = scheduler;
		this.rulesWindow = ruleWindow;
		this.precision = precision.toMillis();
		this.clock = clock;
	}

	@Override
	public void run() {
		try {

			if (scheduler != null) {
				while (true) {
					process();
				}
			}
		} catch (InterruptedException e) {
			//nothing to do
		} catch (Exception e) {
			log.error("Error executing shepherd", e);
		}
	}

	private void process() throws InterruptedException {
		long millis = this.scheduler.calculateWaitingTime(metrics);

		if (millis < 0) {
			Thread.sleep(-millis);
		} else if (millis == 0) {
			checkTimeouts();
		} else {
			log.info("Waiting for next element. Max ms: {}", millis);
			InputValue inputValue = queue.poll(millis, TimeUnit.MILLISECONDS);
			if (inputValue == null) {
				checkTimeouts();
			} else {
				consume(inputValue);
			}
		}
	}

	public void checkTimeouts() {

		log.debug("Dog gonna run for timeouts");

		try (AutoCloseable ac = metrics.rulesTimeoutExecTime()) {
			Iterator<Metadata<K>> it = metadataStorage.values();

			while (it.hasNext()) {
				Metadata<K> metadata = it.next();

				RuleResult<V> ruleResult = rulesWindow.canClose(metadata, new LazyValue(valuesStorage, metadata.getKey()));

				boolean needsToRemoveMetadataForThisKey = postProcess(metadata.getKey(), null, metadata, ruleResult);
				if (needsToRemoveMetadataForThisKey) {
					it.remove();
				}
			}

			if (scheduler != null) {
				scheduler.justExecuted();
			}
		} catch (Exception e) {
			log.error("Error executing timeout rules", e);
		}
	}
}