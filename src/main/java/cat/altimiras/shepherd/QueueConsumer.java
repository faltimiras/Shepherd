package cat.altimiras.shepherd;

import cat.altimiras.shepherd.monitoring.debug.ElementDebugSerializer;
import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.metric.Metric;
import cat.altimiras.shepherd.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public abstract class QueueConsumer<T> implements Runnable {

	protected static Logger log = LoggerFactory.getLogger(QueueConsumer.class);

	protected Storage<Object, T> storage;
	protected final List<Rule<T>> rules;
	protected final BlockingQueue<Element<T>> queue;
	protected final Callback<T> callback;
	protected final RuleExecutor ruleExecutor;
	protected final ReentrantLock storageLock = new ReentrantLock(true);

	public QueueConsumer(Storage<Object, T> storage, List<Rule<T>> rules, BlockingQueue queue, RuleExecutor ruleExecutor, Callback<T> callback) {
		this.storage = storage;
		this.rules = rules;
		this.queue = queue;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;
	}

	public void consume(Element<T> t) {

		try {

			Element<T> element = getOrElse(t.getKey());
			element.addValue(t.getValues().get(0));

			if (rules != null) {
				RuleResult ruleResult = ruleExecutor.execute(element, rules);
				postProcess(t, ruleResult);

				RuleResultPool.release(ruleResult);
			}
			else {
				put(element);
			}
		}
		catch (Exception e) {
			log.error("Error consuming element", e);
		}
	}

	protected abstract Element<T> getOrElse(Object key);

	protected abstract void put(Element<T> toStore);

	protected abstract void remove(Object key);

	protected void postProcess(Element<T> element, RuleResult<T> ruleResult) {

		if (ruleResult.getToKeep() == null) {
			remove(element.getKey());
		}
		else {
			put(ruleResult.getToKeep());
		}

		if (ruleResult.canGroup()) {
			callback.accept(new ArrayList<>(ruleResult.getGroup()));
		}
	}

	protected Map<Stats, Metric> getStats(Level level, ElementDebugSerializer elementDebugSerializer) {
		storageLock.lock();
		try {
			return this.storage.getStats(level, elementDebugSerializer);
		}
		finally {
			storageLock.unlock();
		}
	}
}