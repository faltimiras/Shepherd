package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.DogConsumer;
import cat.altimiras.shepherd.executor.IndependentExecutor;
import cat.altimiras.shepherd.scheduler.BasicScheduler;
import cat.altimiras.shepherd.scheduler.Scheduler;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Shepherd<T> {

	private final KeyExtractor keyExtractor;

	private final LinkedBlockingQueue<Element<T>>[] queues;

	private final ExecutorService pool;

	private final int threads;

	private final Callback<T> callback;

	private final RuleExecutor<T> ruleExecutor;


	private Shepherd(int thread, KeyExtractor keyExtractor, List<Rule<T>> rules, RuleExecutor<T> ruleExecutor, Callback<T> callback, Optional<Dog> dog) {

		this.threads = thread;
		this.queues = new LinkedBlockingQueue[thread];
		Arrays.fill(queues, new LinkedBlockingQueue());

		this.pool = Executors.newFixedThreadPool(thread);
		this.keyExtractor = keyExtractor;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;

		startConsumers(rules, dog);
	}

	private void startConsumers(List<Rule<T>> rules, Optional<Dog> dog) {
		for (int i = 0; i < threads; i++) {
			pool.submit(getConsumer(rules, dog, i));
		}
	}

	private QueueConsumer getConsumer(List<Rule<T>> rules, Optional<Dog> dog, int index) {
		if (dog.isPresent()) {
			Scheduler scheduler = new BasicScheduler(Clock.systemUTC(), dog.get().ttl);
			return new DogConsumer(rules, this.ruleExecutor, this.queues[index], scheduler, dog.get().rulesTimeout, dog.get().ttl, Clock.systemUTC(), dog.get().ruleExecutor, this.callback);
		}
		else {
			return new BasicConsumer(rules, this.queues[index], this.ruleExecutor, this.callback);
		}
	}

	public void add(T t) throws Exception {
		Object key = keyExtractor.key(t);
		Element element = new Element(key, t);
		if (queues.length == 1) {
			queues[0].put(element);
		}
		else {
			int indexQueue = key.hashCode() % threads;
			queues[indexQueue].put(element);
		}
	}

	public void stop() {
		pool.shutdown();
	}

	public static ShepherdBuilder create() {
		return new ShepherdBuilder();
	}

	static class ShepherdBuilder<T> {

		private int thread;
		private KeyExtractor keyExtractor;
		private List<Rule<T>> rules;
		private Callback<T> callback;
		private DogBuilder<T> dogBuilder = null;
		private RuleExecutor ruleExecutor = new IndependentExecutor();

		public ShepherdBuilder basic(int thread, KeyExtractor keyExtractor, Optional<List<Rule<T>>> rules, Callback<T> callback) throws Exception {

			if (thread < 1) {
				throw new IllegalArgumentException("threads must be bigger than 0");
			}
			this.thread = thread;

			if (rules.isPresent()) {
				this.rules = Collections.unmodifiableList(rules.get());
			}

			if (keyExtractor == null) {
				throw new NullPointerException("KeyExtractor can not be null");
			}
			this.keyExtractor = keyExtractor;

			if (callback == null) {
				throw new NullPointerException("callback function can not be null");
			}
			this.callback = callback;
			return this;
		}

		public ShepherdBuilder setRuleExecutor(RuleExecutor ruleExecutor) {
			if (ruleExecutor == null) {
				throw new NullPointerException("Rules executor can not be null. Hint: if you don't need it, just leave it empty");
			}
			this.ruleExecutor = ruleExecutor;
			return this;
		}

		public DogBuilder withDog(Duration ttl, List<Rule<T>> rules) {
			this.dogBuilder = new DogBuilder<T>(this, ttl, rules);
			return this.dogBuilder;
		}

		public Shepherd build() {
			Shepherd shepherd = new Shepherd<T>(thread, keyExtractor, rules, ruleExecutor, callback, Optional.empty());
			return shepherd;
		}

		private Shepherd build(Dog dog) {
			Shepherd shepherd = new Shepherd<T>(thread, keyExtractor, rules, ruleExecutor, callback, Optional.ofNullable(dog));
			return shepherd;
		}
	}

	static class DogBuilder<T> {

		private ShepherdBuilder shepherdBuilder;
		private List<Rule<T>> rules;
		private Duration ttl;
		private RuleExecutor ruleExecutor = new IndependentExecutor();


		public DogBuilder(ShepherdBuilder shepherdBuilder, Duration ttl, List<Rule<T>> rulesTimeout) {
			this.shepherdBuilder = shepherdBuilder;

			if (rulesTimeout == null) {
				throw new NullPointerException("Rules applied on timeout can not be null. Hint: if you don't need them, don't use the Dog");
			}
			if (rulesTimeout.isEmpty()) {
				throw new IllegalArgumentException("At least one rule is mandatory. Hint: if you don't need them, don't use the Dog");
			}
			this.rules = Collections.unmodifiableList(rulesTimeout);

			if (ttl == null) {
				throw new NullPointerException("TTL can not be null");
			}
			this.ttl = ttl;
		}

		public DogBuilder setRuleExecutor(RuleExecutor ruleExecutor) {
			if (ruleExecutor == null) {
				throw new NullPointerException("Rules executor can not be null. Hint: if you don't need it, just leave it empty");
			}
			this.ruleExecutor = ruleExecutor;
			return this;
		}

		public Shepherd build() {
			return this.shepherdBuilder.build(new Dog(rules, ttl, ruleExecutor));
		}
	}

	private static class Dog<T> {
		private List<Rule<T>> rulesTimeout;
		private Duration ttl;
		private RuleExecutor ruleExecutor;

		public Dog(List<Rule<T>> rulesTimeout, Duration ttl, RuleExecutor ruleExecutor) {
			this.ruleExecutor = ruleExecutor;
			this.rulesTimeout = rulesTimeout;
			this.ttl = ttl;
		}
	}

}
