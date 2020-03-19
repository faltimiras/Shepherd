package cat.altimiras.shepherd;

import cat.altimiras.shepherd.executor.IndependentExecutor;
import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.StatsListener;
import cat.altimiras.shepherd.monitoring.debug.ElementDebugSerializer;
import cat.altimiras.shepherd.monitoring.debug.StringElementDebugSerializer;
import cat.altimiras.shepherd.scheduler.BasicScheduler;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryMetadataStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryValuesStorage;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ShepherdBuilder<T, S> {

	private int thread;
	private Function keyExtractor;
	private List<Rule<S>> rules;
	private Consumer<S> callback;
	private DogBuilder<T> dogBuilder = null;
	private MonitoringBuilder monitoringBuilder = null;
	private RuleExecutor<T> ruleExecutor = new IndependentExecutor();
	private Supplier<MetadataStorage> metadataStorageProvider = () -> new InMemoryMetadataStorage();
	private Supplier<ValuesStorage> valuesStorageProvider = () -> new InMemoryValuesStorage();
	private List<StatsListener> statsListeners;

	private ShepherdBuilder() {
	}

	public static ShepherdBuilder create() {
		return new ShepherdBuilder();
	}

	public ShepherdBuilder basic(Optional<List<Rule<S>>> rules, Consumer<S> callback) throws Exception {
		return basic(null, rules, callback);
	}

	public ShepherdBuilder basic(Consumer<S> callback) throws Exception {
		return basic(null, Optional.empty(), callback);
	}

	public ShepherdBuilder basic(Function keyExtractor, Optional<List<Rule<S>>> rules, Consumer<S> callback) throws Exception {

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

	public ShepherdBuilder withRuleExecutor(RuleExecutor ruleExecutor) {
		if (ruleExecutor == null) {
			throw new NullPointerException("Rules executor can not be null. Hint: if you don't need it, just leave it empty");
		}
		this.ruleExecutor = ruleExecutor;
		return this;
	}

	public ShepherdBuilder withMetadataStorageProvider(Supplier<MetadataStorage> metadataStorageProvider) {
		if (metadataStorageProvider == null) {
			throw new NullPointerException("MetadataStorageProvider can not be null. Hint: if you don't need it, just leave it empty");
		}
		this.metadataStorageProvider = metadataStorageProvider;
		return this;
	}

	public ShepherdBuilder withValuesStorageProvider(Supplier<ValuesStorage> valuesStorageProvider) {
		if (valuesStorageProvider == null) {
			throw new NullPointerException("ValuesStorageProvider can not be null. Hint: if you don't need it, just leave it empty");
		}
		this.valuesStorageProvider = valuesStorageProvider;
		return this;
	}


	public ShepherdBuilder threads(int thread) {
		if (thread < 1) {
			throw new IllegalArgumentException("threads must be bigger than 0");
		}
		this.thread = thread;
		return this;
	}

	public ShepherdBuilder addStatListener(StatsListener statsListener) {
		if (statsListener == null) {
			throw new NullPointerException("StatsListener can not be null");
		}

		if (statsListeners == null) {
			statsListeners = new ArrayList<>(1);
		}
		this.statsListeners.add(statsListener);
		return this;
	}

	public DogBuilder withDog(Duration precision, List<Rule<T>> rules) {
		this.dogBuilder = new DogBuilder<T>(this, this.monitoringBuilder, precision, rules);
		return this.dogBuilder;
	}

	public MonitoringBuilder withMonitoring(StatsListener statsListener, Level level) {
		this.monitoringBuilder = new MonitoringBuilder(this, this.dogBuilder, statsListener, level);
		return this.monitoringBuilder;
	}

	public MonitoringBuilder withMonitoring(StatsListener statsListener) {
		this.monitoringBuilder = new MonitoringBuilder(this, this.dogBuilder, statsListener, Level.INFO);
		return this.monitoringBuilder;
	}

	public ShepherdSync buildSync() {
		ShepherdSync shepherd = new ShepherdSync(metadataStorageProvider, valuesStorageProvider, keyExtractor, rules, ruleExecutor, callback, Optional.empty(), Optional.empty());
		return shepherd;
	}

	private ShepherdSync buildSync(Optional<Dog> dog, Optional<Monitoring> monitoring) {
		ShepherdSync shepherd = new ShepherdSync(metadataStorageProvider, valuesStorageProvider, keyExtractor, rules, ruleExecutor, callback, dog, monitoring);
		return shepherd;
	}

	public ShepherdASync build() {
		ShepherdASync shepherd = new ShepherdASync(metadataStorageProvider, valuesStorageProvider, thread, keyExtractor, rules, ruleExecutor, callback, Optional.empty(), Optional.empty());
		return shepherd;
	}

	private ShepherdASync build(Optional<Dog> dog, Optional<Monitoring> monitoring) {

		ShepherdASync shepherd = new ShepherdASync(metadataStorageProvider, valuesStorageProvider, thread, keyExtractor, rules, ruleExecutor, callback, dog, monitoring);
		return shepherd;
	}

	public static class DogBuilder<T> {

		private ShepherdBuilder shepherdBuilder;
		private MonitoringBuilder monitoringBuilder;
		private List<Rule<T>> rules;
		private Duration precision;
		private RuleExecutor ruleExecutor = new IndependentExecutor();
		private Supplier<Scheduler> schedulerProvider;


		public DogBuilder(ShepherdBuilder shepherdBuilder, MonitoringBuilder monitoringBuilder, Duration precision, List<Rule<T>> rulesTimeout) {
			this.shepherdBuilder = shepherdBuilder;
			this.monitoringBuilder = monitoringBuilder;

			if (rulesTimeout == null) {
				throw new NullPointerException("Rules applied on timeout can not be null. Hint: if you don't need them, don't use the Dog");
			}
			if (rulesTimeout.isEmpty()) {
				throw new IllegalArgumentException("At least one rule is mandatory. Hint: if you don't need them, don't use the Dog");
			}
			this.rules = Collections.unmodifiableList(rulesTimeout);

			if (precision == null) {
				throw new NullPointerException("Precision can not be null");
			}
			this.precision = precision;
		}

		public DogBuilder setRuleExecutor(RuleExecutor ruleExecutor) {
			if (ruleExecutor == null) {
				throw new NullPointerException("Rules executor can not be null. Hint: if you don't need it, just leave it empty");
			}
			this.ruleExecutor = ruleExecutor;
			return this;
		}

		public DogBuilder setSchedulerProvider(Supplier<Scheduler> schedulerProvider) {
			if (schedulerProvider == null) {
				throw new NullPointerException("SchedulerProvider can not be null. Hint: if you don't need it, just leave it empty");
			}
			this.schedulerProvider = schedulerProvider;
			return this;
		}

		private Dog buildDog() {
			return new Dog(rules, precision, ruleExecutor, schedulerProvider);
		}


		public MonitoringBuilder withMonitoring(StatsListener statsListener, Level level) {
			this.monitoringBuilder = new MonitoringBuilder(shepherdBuilder, this, statsListener, level);
			return this.monitoringBuilder;
		}

		public MonitoringBuilder withMonitoring(StatsListener statsListener) {
			this.monitoringBuilder = new MonitoringBuilder(shepherdBuilder, this, statsListener, Level.INFO);
			return this.monitoringBuilder;
		}

		public ShepherdASync build() {
			if (monitoringBuilder == null) {
				return this.shepherdBuilder.build(Optional.of(new Dog(rules, precision, ruleExecutor, schedulerProvider)), Optional.empty());
			} else {
				return this.shepherdBuilder.build(Optional.of(new Dog(rules, precision, ruleExecutor, schedulerProvider)), Optional.of(monitoringBuilder.buildMonitoring()));
			}
		}

		public ShepherdSync buildSync() {
			if (monitoringBuilder == null) {
				return this.shepherdBuilder.buildSync(Optional.of(new Dog(rules, precision, ruleExecutor, schedulerProvider)), Optional.empty());
			} else {
				return this.shepherdBuilder.buildSync(Optional.of(new Dog(rules, precision, ruleExecutor, schedulerProvider)), Optional.of(monitoringBuilder.buildMonitoring()));
			}
		}
	}

	static class Dog<T> {
		private List<Rule<T>> rulesTimeout;
		private Duration precision;
		private RuleExecutor ruleExecutor;
		private Supplier<Scheduler> schedulerProvider;

		public Dog(List<Rule<T>> rulesTimeout, Duration precision, RuleExecutor ruleExecutor, Supplier<Scheduler> schedulerProvider) {
			this.ruleExecutor = ruleExecutor;
			this.rulesTimeout = rulesTimeout;
			this.precision = precision;
			if (schedulerProvider == null) {
				this.schedulerProvider = () -> new BasicScheduler(Clock.systemUTC(), precision);
			} else {
				this.schedulerProvider = schedulerProvider;
			}
		}

		public List<Rule<T>> getRulesTimeout() {
			return rulesTimeout;
		}

		public Duration getPrecision() {
			return precision;
		}

		public RuleExecutor getRuleExecutor() {
			return ruleExecutor;
		}

		public Supplier<Scheduler> getSchedulerProvider() {
			return schedulerProvider;
		}
	}

	public static class MonitoringBuilder {

		private ShepherdBuilder shepherdBuilder;
		private DogBuilder dogBuilder;
		private List<StatsListener> statsListeners;
		private ElementDebugSerializer elementDebugSerializer = new StringElementDebugSerializer();
		private Duration every = Duration.ofMinutes(1l);
		private Level level;
		private boolean enabled = true;

		public MonitoringBuilder(ShepherdBuilder shepherdBuilder, DogBuilder dogBuilder, StatsListener statsListener, Level level) {
			this.shepherdBuilder = shepherdBuilder;
			this.dogBuilder = dogBuilder;
			this.statsListeners = new ArrayList<>(1);
			this.statsListeners.add(statsListener);
			this.level = level;
		}

		public MonitoringBuilder every(Duration every) {
			this.every = every;
			return this;
		}

		public MonitoringBuilder addListener(StatsListener statsListener) {
			this.statsListeners.add(statsListener);
			return this;
		}

		public MonitoringBuilder addListener(List<StatsListener> statsListeners) {
			this.statsListeners.addAll(statsListeners);
			return this;
		}

		public MonitoringBuilder addElementDebug(ElementDebugSerializer elementDebugSerializer) {
			this.elementDebugSerializer = elementDebugSerializer;
			return this;
		}

		public MonitoringBuilder level(Level level) {
			this.level = level;
			return this;
		}

		public MonitoringBuilder enabled(boolean enabled) {
			this.enabled = enabled;
			return this;
		}

		public DogBuilder withDog(Duration precision, List<Rule> rules) {
			this.dogBuilder = new DogBuilder(shepherdBuilder, this, precision, rules);
			return this.dogBuilder;
		}

		private Monitoring buildMonitoring() {
			return new Monitoring(statsListeners, every, level, enabled, elementDebugSerializer);
		}

		public ShepherdASync build() {
			if (dogBuilder == null) {
				return this.shepherdBuilder.build(Optional.empty(), Optional.of(new Monitoring(statsListeners, every, level, enabled, elementDebugSerializer)));
			} else {
				return this.shepherdBuilder.build(Optional.of(dogBuilder.buildDog()), Optional.of(new Monitoring(statsListeners, every, level, enabled, elementDebugSerializer)));
			}
		}

		public ShepherdSync buildSync() {
			if (dogBuilder == null) {
				return this.shepherdBuilder.buildSync(Optional.empty(), Optional.of(new Monitoring(statsListeners, every, level, enabled, elementDebugSerializer)));
			} else {
				return this.shepherdBuilder.buildSync(Optional.of(dogBuilder.buildDog()), Optional.of(new Monitoring(statsListeners, every, level, enabled, elementDebugSerializer)));
			}
		}
	}

	static class Monitoring {
		private boolean enabled = true;
		private List<StatsListener> statsListeners;
		private ElementDebugSerializer elementDebugSerializer;
		private Duration every;
		private Level level;

		public Monitoring(List<StatsListener> statsListeners, Duration every, Level level, boolean enabled, ElementDebugSerializer elementDebugSerializer) {
			this.statsListeners = statsListeners;
			this.every = every;
			this.level = level;
			this.enabled = enabled;
			this.elementDebugSerializer = elementDebugSerializer;
		}

		public List<StatsListener> getStatsListeners() {
			return statsListeners;
		}

		public Duration getEvery() {
			return every;
		}

		public Level getLevel() {
			return level;
		}

		public boolean isEnabled() {
			return enabled;
		}

		public ElementDebugSerializer getElementDebugSerializer() {
			return elementDebugSerializer;
		}
	}
}