package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public abstract class QueueConsumer<K, V, S> implements Runnable {

	protected static Logger log = LoggerFactory.getLogger(QueueConsumer.class);

	protected final List<Rule<V, S>> rules;
	protected final BlockingQueue<InputValue<K, V>> queue;
	protected final Consumer<S> callback;
	protected final RuleExecutor<V, S> ruleExecutor;
	protected final Metrics metrics;
	protected ValuesStorage<K, V, S> valuesStorage;
	protected MetadataStorage<K> metadataStorage;

	public QueueConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V, S>> rules, BlockingQueue<InputValue<K, V>> queue, RuleExecutor<V, S> ruleExecutor, Consumer<S> callback, Metrics metrics) {
		this.metadataStorage = metadataStorage;
		this.valuesStorage = valuesStorage;
		this.rules = rules;
		this.queue = queue;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.metrics = metrics;
	}

	public void consume(InputValue<K, V> t) {

		try (AutoCloseable ac = metrics.rulesExecTime()) {
			metrics.pendingDec();
			Metadata<K> metadata = metadataStorage.get(t.getKey());
			if (metadata == null) {
				metadata = new Metadata(t.getKey(), t.getIngestionTs());
				metadataStorage.put(t.getKey(), metadata);
			}
			metadata.setLastElementTs(t.getIngestionTs());

			if (rules != null) {
				RuleResult ruleResult = ruleExecutor.execute(metadata, t.getValue(), new LazyValues(valuesStorage, t.getKey()), rules);
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
		}

		if (value != null && ruleResult.getAppend() == -1) {
			valuesStorage.append(key, value);
			metadata.incElementsCount();
			needsToRemoveMetadataForThisKey = false; //cancel metadata remove
		}

		if (ruleResult.canClose()) {
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
		}

		if (value != null && ruleResult.getAppend() == 1) {
			valuesStorage.append(key, value);
			metadata.incElementsCount();
			needsToRemoveMetadataForThisKey = false; //cancel metadata remove
		}

		if (ruleResult.getToKeep() != null) {
			valuesStorage.override(key, ruleResult.getToKeep());
			//metadata.setLastElementTs(ruleResult.getToKeep().size());
		}

		return needsToRemoveMetadataForThisKey;
	}
}