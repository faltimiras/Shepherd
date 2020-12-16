package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QueueConsumer<K, V, S> implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(QueueConsumer.class);

	protected final Clock clock;

	protected final List<Rule<V, S>> rules;

	protected final BlockingQueue<InputValue<K, V>> queue;

	protected final Consumer<S> callback;

	protected final RuleExecutor<V, S> ruleExecutor;

	protected final Metrics metrics;

	protected ValuesStorage<K, V, S> valuesStorage;

	protected MetadataStorage<K> metadataStorage;

	public QueueConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V, S>> rules, BlockingQueue<InputValue<K, V>> queue, RuleExecutor<V, S> ruleExecutor, Consumer<S> callback, Metrics metrics, Clock clock) {
		this.metadataStorage = metadataStorage;
		this.valuesStorage = valuesStorage;
		this.rules = rules;
		this.queue = queue;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.metrics = metrics;
		this.clock = clock;
	}

	public void consume(InputValue<K, V> t) {
		log.debug("Processing {}", t);
		try (AutoCloseable ac = metrics.rulesExecTime()) {
			metrics.pendingDec();
			Metadata<K> metadata = metadataStorage.get(t.getKey());
			if (metadata == null) {
				log.debug("New element with key {} processed", t.getKey());
				metadata = new Metadata(t.getKey(), t.getTimestamp(), clock);
				metadataStorage.put(t.getKey(), metadata);
			}
			metadata.setLastElementTs(t.getTimestamp());

			if (rules != null) {
				log.debug("Starting execute streaming rules for element {}", t);
				RuleResult ruleResult = ruleExecutor.execute(metadata, t.getValue(), new LazyValues(valuesStorage, t.getKey()), rules);
				log.debug("Streaming rules for element {} executed with result: {}", t.getKey(), ruleResult);
				boolean needsToRemoveMetadataForThisKey = postProcess(t.getKey(), t.getValue(), metadata, ruleResult);
				if (needsToRemoveMetadataForThisKey) {
					metadataStorage.remove(t.getKey());
				}
				RuleResultPool.release(ruleResult);
			} else {
				valuesStorage.append(t.getKey(), t.getValue());
			}
		} catch (Exception e) {
			log.error("Error consuming element", e);
		}
	}

	protected boolean postProcess(K key, V value, Metadata metadata, RuleResult<S> ruleResult) {

		boolean needsToRemoveMetadataForThisKey = false;
		if (ruleResult.getDiscard() == -1) {
			valuesStorage.remove(key);
			metadata.resetElementsCount();
			needsToRemoveMetadataForThisKey = true;
			log.debug("Elements with key {} has been discarded", key);
		}

		if (value != null && ruleResult.getAppend() == -1) {
			valuesStorage.append(key, value);
			metadata.incElementsCount();
			needsToRemoveMetadataForThisKey = false; //cancel metadata remove
			log.debug("Element {} with key {} has been appended (before grouping if grouping)", value, key);
		}

		if (ruleResult.canClose()) {
			log.debug("Elements with key {} has been grouped", key);
			//output format depends on how storage handles it
			if (ruleResult.getGroup() != null) {
				callback.accept(ruleResult.getGroup());
			} else {
				S s = valuesStorage.get(key);
				if (s != null) {
					callback.accept(s);
				}
			}
		}

		if (ruleResult.getDiscard() == 1) {
			valuesStorage.remove(key);
			metadata.resetElementsCount();
			needsToRemoveMetadataForThisKey = true;
			log.debug("Elements with key {} has been discarded, after grouping (if grouping)", key);
		}

		if (value != null && ruleResult.getAppend() == 1) {
			valuesStorage.append(key, value);
			metadata.incElementsCount();
			needsToRemoveMetadataForThisKey = false; //cancel metadata remove
			log.debug("Element {} with key {} has been appended, after grouping (if grouping)", value, key);
		}

		if (ruleResult.getToKeep() != null) {
			valuesStorage.override(key, ruleResult.getToKeep());
			log.debug("Keeping elements with key {}", key);
		}

		return needsToRemoveMetadataForThisKey;
	}
}