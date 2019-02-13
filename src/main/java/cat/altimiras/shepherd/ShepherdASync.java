package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.DogConsumer;
import cat.altimiras.shepherd.scheduler.BasicScheduler;
import cat.altimiras.shepherd.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ShepherdASync<T> extends ShepherdBase<T> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdASync.class);

	private final LinkedBlockingQueue<Element<T>>[] queues;

	private final ExecutorService pool;

	private final int threads;

	private final boolean hasDog;

	ShepherdASync(int thread, KeyExtractor keyExtractor, List<Rule<T>> rules, RuleExecutor<T> ruleExecutor, Callback<T> callback, Optional<ShepherdBuilder.Dog> dog, Optional<ShepherdBuilder.Monitoring> monitoring) {

		super(keyExtractor, callback, ruleExecutor, thread, dog.isPresent(), monitoring);

		this.threads = thread;
		this.queues = new LinkedBlockingQueue[thread];
		this.pool = Executors.newFixedThreadPool(thread);
		this.hasDog = dog.isPresent();

		initializeQueues();
		startConsumers(rules, dog);
	}

	private void initializeQueues() {
		for (int i = 0; i < threads; i++) {
			queues[i] = new LinkedBlockingQueue();
		}
	}

	private void startConsumers(List<Rule<T>> rules, Optional<ShepherdBuilder.Dog> dog) {
		for (int i = 0; i < threads; i++) {
			QueueConsumer qc = getAsyncConsumer(rules, dog, i);
			consumers.add(qc);
			pool.submit(qc);
		}
	}

	private QueueConsumer getAsyncConsumer(List<Rule<T>> rules, Optional<ShepherdBuilder.Dog> dog, int index) {
		if (dog.isPresent()) {
			Scheduler scheduler = new BasicScheduler(Clock.systemUTC(), dog.get().getTtl());
			return new DogConsumer(rules, this.ruleExecutor, this.queues[index], scheduler, dog.get().getRulesTimeout(), dog.get().getTtl(), Clock.systemUTC(), dog.get().getRuleExecutor(), this.callback);
		}
		else {
			return new BasicConsumer(rules, this.queues[index], this.ruleExecutor, this.callback);
		}
	}

	public boolean add(T t, Instant timestmap) {
		try {
			Object key = keyExtractor.key(t);
			if (key == null) {
				log.error("Extracted key == null, discarding object");
				log.info("Element discarded {}", t);
				return false;
			}
			else {
				Element element = timestmap == null ? new Element(key, t) : new Element(key, t, timestmap);
				if (queues.length == 1) {
					queues[0].put(element);
				}
				else {
					int indexQueue = key.hashCode() % threads;
					//hashcode can be negative
					queues[indexQueue < 0 ? -indexQueue : indexQueue].put(element);
				}
			}
			return true;
		}
		catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	public boolean add(T t, Long timestmap) {
		return add(t, Instant.ofEpochMilli(timestmap));
	}

	public boolean add(T t) {
		return add(t, (Instant) null);
	}

	public void stop() {
		pool.shutdown();
	}

	public boolean areQueuesEmpty() {

		for(int i = 0; i< queues.length; i++){
			if (!queues[i].isEmpty()){
				return false;
			}
		}
		return true;
	}

}
