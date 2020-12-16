package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Metrics;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.rules.RuleWindow;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowedConsumer<K, V, S> extends QueueConsumer<K, V, S> {

	private static final Logger log = LoggerFactory.getLogger(WindowedConsumer.class);

	private final Scheduler scheduler;

	private final RuleWindow<V, S> rulesWindow;

	public WindowedConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V, S>> rules, RuleExecutor<V, S> ruleExecutor, BlockingQueue<InputValue<K, V>> queue, Scheduler scheduler, RuleWindow ruleWindow, Consumer<S> callback, Metrics metrics, Clock clock) {
		super(metadataStorage, valuesStorage, rules, queue, ruleExecutor, callback, metrics, clock);
		this.scheduler = scheduler;
		this.rulesWindow = ruleWindow;
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
			checkWindows();
		} else {
			log.trace("Waiting for next element. Max ms: {}", millis);
			InputValue inputValue = queue.poll(millis, TimeUnit.MILLISECONDS);
			if (inputValue == null) {
				checkWindows();
			} else {
				consume(inputValue);
			}
		}
	}

	public void checkWindows() {
		Instant start = null;
		if (log.isDebugEnabled()) {
			start = clock.instant();
			log.debug("Checking opened windows at {}", start);
		}
		try (AutoCloseable ac = metrics.ruleWindowExecTime()) {
			Iterator<Metadata<K>> it = metadataStorage.values();

			while (it.hasNext()) {
				Metadata<K> metadata = it.next();
				log.debug("Checking window rule for key {}", metadata.getKey());
				RuleResult<S> ruleResult = rulesWindow.canClose(metadata, new LazyValues(valuesStorage, metadata.getKey()));
				log.debug("Checked window rule for key {} with result {}", metadata.getKey(), ruleResult);
				boolean needsToRemoveMetadataForThisKey = postProcess(metadata.getKey(), null, metadata, ruleResult);
				if (needsToRemoveMetadataForThisKey) {
					it.remove();
				}
			}

			if (scheduler != null) {
				scheduler.justExecuted();
			}

			if (log.isDebugEnabled()) {
				log.debug("Finished checking window rules. Process it took: {}ms", start.minusMillis(clock.instant().toEpochMilli()).toEpochMilli());
			}
		} catch (Exception e) {
			log.error("Error closing windows", e);
		}
	}
}