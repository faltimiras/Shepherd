package cat.altimiras.shepherd;

import cat.altimiras.shepherd.executor.IndependentExecutor;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryMetadataStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryValuesStorage;
import com.codahale.metrics.MetricRegistry;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShepherdBuilder<T, S> {

	private int thread;
	private Function keyExtractor;
	private List<Rule<S>> rules;
	private Consumer<S> callback;
	private WindowBuilder<T> windowBuilder = null;
	private MetricRegistry metrics = null;
	private RuleExecutor<T> ruleExecutor = new IndependentExecutor();
	private Supplier<MetadataStorage> metadataStorageProvider = InMemoryMetadataStorage::new;
	private Supplier<ValuesStorage> valuesStorageProvider = InMemoryValuesStorage::new;
	private final static Clock clock = Clock.systemUTC();

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
		rules.ifPresent(ruleList -> this.rules = Collections.unmodifiableList(ruleList));

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

	public WindowBuilder withWindow(Duration precision, RuleWindow rule) {
		this.windowBuilder = new WindowBuilder<T>(this, precision, rule);
		return this.windowBuilder;
	}

	public ShepherdBuilder withMonitoring(MetricRegistry metrics) {
		if (metrics == null) {
			throw new NullPointerException("MetricRegistry can not be null");
		}
		this.metrics = metrics;
		return this;
	}


	public ShepherdSync buildSync() {
		return new ShepherdSync(metadataStorageProvider, valuesStorageProvider, keyExtractor, rules, ruleExecutor, callback, windowBuilder.buildWindow(), new Metrics(metrics), clock);
	}

	private ShepherdSync buildSync(Window window) {
		return new ShepherdSync(metadataStorageProvider, valuesStorageProvider, keyExtractor, rules, ruleExecutor, callback, window, new Metrics(metrics), clock);
	}

	public ShepherdASync build() {
		return new ShepherdASync(metadataStorageProvider, valuesStorageProvider, thread, keyExtractor, rules, ruleExecutor, callback, null, new Metrics(metrics), clock);
	}

	private ShepherdASync build(Window window) {

		return new ShepherdASync(metadataStorageProvider, valuesStorageProvider, thread, keyExtractor, rules, ruleExecutor, callback, window, new Metrics(metrics), clock);
	}

	public static class WindowBuilder<T> {

		private ShepherdBuilder shepherdBuilder;
		private RuleWindow rule;
		private Duration precision;
		private Supplier<Scheduler> schedulerProvider;


		public WindowBuilder(ShepherdBuilder shepherdBuilder, Duration precision, RuleWindow ruleWindow) {
			this.shepherdBuilder = shepherdBuilder;

			if (ruleWindow == null) {
				throw new NullPointerException("Rule applied to close the Window can not be null. Hint: if you don't need them, don't use the Window");
			}

			this.rule = ruleWindow;

			if (precision == null) {
				throw new NullPointerException("Precision can not be null");
			}
			this.precision = precision;
		}


		public WindowBuilder setSchedulerProvider(Supplier<Scheduler> schedulerProvider) {
			if (schedulerProvider == null) {
				throw new NullPointerException("SchedulerProvider can not be null. Hint: if you don't need it, just leave it empty");
			}
			this.schedulerProvider = schedulerProvider;
			return this;
		}

		private Window buildWindow() {
			return new Window(rule, precision, schedulerProvider);
		}


		public ShepherdASync build() {
			return this.shepherdBuilder.build(new Window(rule, precision, schedulerProvider));
		}

		public ShepherdSync buildSync() {
			return this.shepherdBuilder.buildSync(new Window(rule, precision, schedulerProvider));
		}
	}

}