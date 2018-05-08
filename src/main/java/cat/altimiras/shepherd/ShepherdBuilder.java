package cat.altimiras.shepherd;

import cat.altimiras.shepherd.executor.IndependentExecutor;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ShepherdBuilder<T> {

	private int thread;
	private KeyExtractor keyExtractor;
	private List<Rule<T>> rules;
	private Callback<T> callback;
	private DogBuilder<T> dogBuilder = null;
	private RuleExecutor ruleExecutor = new IndependentExecutor();

	private ShepherdBuilder() {

	}

	public static ShepherdBuilder create() {
		return new ShepherdBuilder();
	}

	public ShepherdBuilder basic(KeyExtractor keyExtractor, Optional<List<Rule<T>>> rules, Callback<T> callback) throws Exception {

		this.thread = 1;
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

	public ShepherdBuilder threads(int thread) {
		if (thread < 1) {
			throw new IllegalArgumentException("threads must be bigger than 0");
		}
		this.thread = thread;
		return this;
	}

	public DogBuilder withDog(Duration ttl, List<Rule<T>> rules) {
		this.dogBuilder = new DogBuilder<T>(this, ttl, rules);
		return this.dogBuilder;
	}

	public ShepherdSync buildSync() {
		ShepherdSync shepherd = new ShepherdSync<T>(keyExtractor, rules, ruleExecutor, callback, Optional.empty());
		return shepherd;
	}

	public ShepherdSync buildSync(Dog dog) {
		ShepherdSync shepherd = new ShepherdSync<T>(keyExtractor, rules, ruleExecutor, callback, Optional.ofNullable(dog));
		return shepherd;
	}

	public ShepherdASync build() {
		ShepherdASync shepherd = new ShepherdASync<T>(thread, keyExtractor, rules, ruleExecutor, callback, Optional.empty());
		return shepherd;
	}

	private ShepherdASync build(Dog dog) {
		ShepherdASync shepherd = new ShepherdASync<T>(thread, keyExtractor, rules, ruleExecutor, callback, Optional.ofNullable(dog));
		return shepherd;
	}

	public static class DogBuilder<T> {

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

		public ShepherdASync build() {
			return this.shepherdBuilder.build(new Dog(rules, ttl, ruleExecutor));
		}

		public ShepherdSync buildSync() {
			return this.shepherdBuilder.buildSync(new Dog(rules, ttl, ruleExecutor));
		}
	}

	static class Dog<T> {
		private List<Rule<T>> rulesTimeout;
		private Duration ttl;
		private RuleExecutor ruleExecutor;

		public Dog(List<Rule<T>> rulesTimeout, Duration ttl, RuleExecutor ruleExecutor) {
			this.ruleExecutor = ruleExecutor;
			this.rulesTimeout = rulesTimeout;
			this.ttl = ttl;
		}

		public List<Rule<T>> getRulesTimeout() {
			return rulesTimeout;
		}

		public Duration getTtl() {
			return ttl;
		}

		public RuleExecutor getRuleExecutor() {
			return ruleExecutor;
		}
	}
}