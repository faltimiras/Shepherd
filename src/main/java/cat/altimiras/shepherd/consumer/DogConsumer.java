package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.Callback;
import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.LinkedMapStorage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DogConsumer<T> extends QueueConsumer<T> {

	private final Scheduler scheduler;
	private final List<Rule<T>> rulesTimeout;
	private final Duration ttl;
	private final Clock clock;
	private final RuleExecutor<T> ruleExecutorTimeout;


	public DogConsumer(List<Rule<T>> rules, RuleExecutor<T> ruleExecutor, BlockingQueue<Element<T>> queue, Scheduler scheduler, List<Rule<T>> rulesTimeout, Duration ttl, Clock clock, RuleExecutor<T> ruleTimeoutExecutor, Callback<T> callback) {
		super(new LinkedMapStorage(), rules, queue, ruleExecutor, callback);
		this.scheduler = scheduler;
		this.rulesTimeout = rulesTimeout;
		this.ttl = ttl;
		this.clock = clock;
		this.ruleExecutorTimeout = ruleTimeoutExecutor;
	}

	@Override
	public void run() {
		try {
			if (scheduler != null) {
				while (true) {

					long millis = this.scheduler.calculateWaitingTime();

					if (millis <= 0) {
						checkTimeouts(false);
					} else {
						log.info("Waiting for next element. Max ms: {}", millis);
						Element<T> element = queue.poll(millis, TimeUnit.MILLISECONDS);
						if (element == null) {
							checkTimeouts(false);
						} else {
							consume(element);
						}
					}
				}
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

	public void checkTimeouts(boolean force) {

		storageLock.lock();
		try {

			log.debug("Dog gonna run for timeouts");

			Iterator<Element<T>> it = storage.values();

			Instant now = clock.instant();

			boolean stop = false;
			while (it.hasNext() && !stop) {
				Element<T> element = it.next();

				Duration diff = Duration.between(element.getCreationTs(), now);
				if (force || diff.compareTo(ttl) > 0) {
					RuleResult ruleResult = ruleExecutorTimeout.execute(element, rulesTimeout);
					if (ruleResult.getToKeep() != null) {
						storage.put(ruleResult.getToKeep().getKey(), ruleResult.getToKeep());
					} else {
						it.remove();
					}

					if (ruleResult.canGroup()) {
						callback.accept(ruleResult.getGroup());
					}
				} else {
					stop = true; //elements are ordered by creation time. Make no sense to continue if one is not older, next one wont be
				}
			}

			if (scheduler != null) {
				scheduler.justExecuted();
			}
		} finally {
			storageLock.unlock();
		}
	}
}
