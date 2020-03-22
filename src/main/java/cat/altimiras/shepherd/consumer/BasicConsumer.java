package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.Metrics;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class BasicConsumer<K, V, S> extends QueueConsumer<K, V, S> {

	public BasicConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V>> rules, BlockingQueue<InputValue<K, V>> queue, RuleExecutor<V> ruleExecutor, Consumer<S> callback, Metrics metrics) {
		super(metadataStorage, valuesStorage, rules, queue, ruleExecutor, callback, metrics);
	}

	@Override
	public void run() {
		try {
			while (true) {
				consume(queue.take());
			}
		} catch (InterruptedException e) {
			//nothing to do
		} catch (Exception e) {
			log.error("Errr consuming objects", e);
		}
	}
}