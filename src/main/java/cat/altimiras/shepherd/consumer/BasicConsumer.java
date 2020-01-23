package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.Callback;
import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.storage.MapStorage;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class BasicConsumer<T> extends QueueConsumer<T> {

	public BasicConsumer(List<Rule<T>> rules, BlockingQueue<Element<T>> queue, RuleExecutor<T> ruleExecutor, Callback<T> callback) {
		super(new MapStorage(), rules, queue, ruleExecutor, callback);
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

	@Override
	protected Element<T> getOrElse(Object key) {
		return storage.getOrDefault(key, new Element<>(key));
	}

	@Override
	protected void put(Element<T> toStore) {
		storage.put(toStore.getKey(), toStore);
	}

	@Override
	protected void remove(Object key) {
		storageLock.lock();
		try {
			storage.remove(key);
		} finally {
			storageLock.unlock();
		}
	}
}