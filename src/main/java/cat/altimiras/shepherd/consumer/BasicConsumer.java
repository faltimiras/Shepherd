package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class BasicConsumer<T, S> extends QueueConsumer<T, S> {

	public BasicConsumer(MetadataStorage metadataStorage, ValuesStorage valuesStorage, List<Rule<T>> rules, BlockingQueue<InputValue<T>> queue, RuleExecutor<T> ruleExecutor, Consumer<S> callback) {
		super(metadataStorage, valuesStorage, rules, queue, ruleExecutor, callback);
	}

	@Override
	public void run() {
		try {
			while (true) {
				consume(queue.take());
			}
		} catch (InterruptedException e) {
			//nothing to do
		}
	}
}