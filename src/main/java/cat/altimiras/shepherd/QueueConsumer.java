package cat.altimiras.shepherd;

import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public abstract class QueueConsumer<K, T, S> implements Runnable {

	protected static Logger log = LoggerFactory.getLogger(QueueConsumer.class);

	protected ValuesStorage<K, T, S> valuesStorage;
	protected MetadataStorage<K> metadataStorage;
	protected final List<Rule<T>> rules;
	protected final BlockingQueue<InputValue<K, T>> queue;
	protected final Consumer<S> callback;
	protected final RuleExecutor<T> ruleExecutor;
	protected final Metrics metrics;

	public QueueConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, T, S> valuesStorage, List<Rule<T>> rules, BlockingQueue<InputValue<K, T>> queue, RuleExecutor<T> ruleExecutor, Consumer<S> callback, Metrics metrics) {
		this.metadataStorage = metadataStorage;
		this.valuesStorage = valuesStorage;
		this.rules = rules;
		this.queue = queue;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
		this.metrics = metrics;
	}

	public void consume(InputValue<K, T> t) {

		try (AutoCloseable ac = metrics.rulesExecTime()){
			metrics.pendingDec();
			Metadata<K> metadata = metadataStorage.get(t.getKey());
			if (metadata == null) {
				metadata = new Metadata(t.getKey(), t.getIngestionTs());
				metadataStorage.put(t.getKey(), metadata);
			}
			metadata.setLastElementTs(t.getIngestionTs());

			if (rules != null) {
				RuleResult ruleResult = ruleExecutor.execute(metadata, t.getValue(), new LazyValue(valuesStorage, t.getKey()), rules);
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

	protected boolean postProcess(K key, T value, Metadata metadata, RuleResult<T> ruleResult) {

		boolean needsToRemoveMetadataForThisKey = false;
		if (ruleResult.getDiscard() == -1) {
			valuesStorage.remove(key);
			needsToRemoveMetadataForThisKey = true;
		}

		if (value != null && ruleResult.getAppend() == -1) {
			valuesStorage.append(key, value);
			metadata.incElementsCount();
		}

		if (ruleResult.canClose()) {
			//output format depends on how storage handles it
			if (ruleResult.getGroup() != null) {
				valuesStorage.override(key, ruleResult.getToKeep());
				metadata.setLastElementTs(ruleResult.getToKeep().size());
			}
			callback.accept(valuesStorage.publish(key));
		}

		if (value != null && ruleResult.getAppend() == 1) {
			valuesStorage.append(key, value);
			metadata.incElementsCount();
		}

		if (ruleResult.getDiscard() == 1) {
			valuesStorage.remove(key);
			needsToRemoveMetadataForThisKey = true;
		}

		if (ruleResult.getToKeep() != null) {
			valuesStorage.override(key, ruleResult.getToKeep());
			metadata.setLastElementTs(ruleResult.getToKeep().size());
		}

		return needsToRemoveMetadataForThisKey;
	}
}