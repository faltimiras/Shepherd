package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.Metrics;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicConsumer<K, V, S> extends QueueConsumer<K, V, S> {

	private static final Logger log = LoggerFactory.getLogger(BasicConsumer.class);

	public BasicConsumer(MetadataStorage<K> metadataStorage, ValuesStorage<K, V, S> valuesStorage, List<Rule<V, S>> rules, BlockingQueue<InputValue<K, V>> queue, RuleExecutor<V, S> ruleExecutor, Consumer<S> callback, Metrics metrics, Clock clock) {
		super(metadataStorage, valuesStorage, rules, queue, ruleExecutor, callback, metrics, clock);
	}

	@Override
	public void run() {
		try {
			while (true) {
				consume(queue.take()); //queue blocks and "sleeps" if empty
			}
		} catch (InterruptedException e) {
			//nothing to do
		} catch (Exception e) {
			log.error("Error consuming objects", e);
		}
	}
}