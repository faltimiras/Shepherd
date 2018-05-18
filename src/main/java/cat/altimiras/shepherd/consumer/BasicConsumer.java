package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.Callback;
import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.MapExtractor;
import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.metric.Metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class BasicConsumer<T> extends QueueConsumer<T> {

	private Map<Object, Element<T>> storage = new HashMap<>();

	public BasicConsumer(List<Rule<T>> rules, BlockingQueue<Element<T>> queue, RuleExecutor<T> ruleExecutor, Callback<T> callback) {
		super(rules, queue, ruleExecutor, callback);
	}

	@Override
	public void run() {
		try {
			while (true) {
				consume(queue.take());
			}
		}
		catch (InterruptedException e) {
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
		}
		finally {
			storageLock.unlock();
		}
	}

	@Override
	protected Map<Stats, Metric> getStats(Level level) {
		storageLock.lock();
		try {
			return MapExtractor.extract(storage, level);
		}
		finally {
			storageLock.unlock();
		}
	}
}
