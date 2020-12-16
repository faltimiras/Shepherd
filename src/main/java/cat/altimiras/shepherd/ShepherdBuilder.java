package cat.altimiras.shepherd;

import cat.altimiras.shepherd.executor.CloseOrLastExecutor;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.rules.RuleWindow;
import cat.altimiras.shepherd.scheduler.Scheduler;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryListValuesStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryMetadataStorage;
import com.codahale.metrics.MetricRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShepherdBuilder<V, S> {

	private final static Clock clock = Clock.systemUTC();

	private int thread;

	private Function keyExtractor;

	private List<Rule<V, S>> rules;

	private Consumer<S> callback;

	private WindowBuilder<V> windowBuilder = null;

	private MetricRegistry metrics = null;

	private RuleExecutor<V, S> ruleExecutor = new CloseOrLastExecutor();

	private Supplier<MetadataStorage> metadataStorageProvider = InMemoryMetadataStorage::new;

	private Supplier<ValuesStorage> valuesStorageProvider = InMemoryListValuesStorage::new;

	private ShepherdBuilder() {
	}

	public static ShepherdBuilder create() {
		return new ShepherdBuilder();
	}

	public ShepherdBuilder basic(Consumer<S> callback, List<Rule<V, S>> rules) throws Exception {
		return basic(null, callback, rules);
	}

	public ShepherdBuilder basic(Function keyExtractor, Consumer<S> callback, List<Rule<V, S>> rules) throws Exception {
		Objects.requireNonNull(callback, "callback function can not be null");
		this.thread = 1;
		if (rules != null && !rules.isEmpty()) {
			this.rules = Collections.unmodifiableList(rules);
		}
		this.keyExtractor = keyExtractor;
		this.callback = callback;
		return this;
	}

	public ShepherdBuilder basic(Consumer<S> callback, Rule<V, S>... rules) throws Exception {
		return basic(null, callback, Arrays.asList(rules));
	}

	public ShepherdBuilder basic(Function keyExtractor, Consumer<S> callback, Rule<V, S>... rules) throws Exception {
		return basic(keyExtractor, callback, Arrays.asList(rules));
	}

	public ShepherdBuilder basic(Consumer<S> callback) throws Exception {
		return basic(null, callback, (List) null);
	}

	public ShepherdBuilder withRuleExecutor(RuleExecutor ruleExecutor) {
		Objects.requireNonNull(ruleExecutor, "Rules executor can not be null. Hint: if you don't need it, just leave it empty");
		this.ruleExecutor = ruleExecutor;
		return this;
	}

	public ShepherdBuilder withMetadataStorageProvider(Supplier<MetadataStorage> metadataStorageProvider) {
		Objects.requireNonNull(metadataStorageProvider, "MetadataStorageProvider can not be null. Hint: if you don't need it, just leave it empty");
		this.metadataStorageProvider = metadataStorageProvider;
		return this;
	}

	public ShepherdBuilder withValuesStorageProvider(Supplier<ValuesStorage> valuesStorageProvider) {
		Objects.requireNonNull(valuesStorageProvider, "ValuesStorageProvider can not be null. Hint: if you don't need it, just leave it empty");
		this.valuesStorageProvider = valuesStorageProvider;
		return this;
	}

	public ShepherdBuilder withMonitoring(MetricRegistry metrics) {
		if (metrics == null) {
			throw new NullPointerException("MetricRegistry can not be null");
		}
		this.metrics = metrics;
		return this;
	}

	public ShepherdSync buildSync() {
		Window window = windowBuilder == null ? null : windowBuilder.buildWindow();
		return new ShepherdSync(metadataStorageProvider, valuesStorageProvider, keyExtractor, rules, ruleExecutor, callback, window, new Metrics(metrics), clock);
	}

	private ShepherdSync buildSync(Window window) {
		if (thread != 1) {
			throw new IllegalArgumentException("Sync Shepherd must be mono thread");
		}
		return new ShepherdSync(metadataStorageProvider, valuesStorageProvider, keyExtractor, rules, ruleExecutor, callback, window, new Metrics(metrics), clock);
	}

	public ShepherdASync build() {
		Window window = windowBuilder == null ? null : windowBuilder.buildWindow();
		return build(window);
	}

	private ShepherdASync build(Window window) {
		return new ShepherdASync(metadataStorageProvider, valuesStorageProvider, thread, keyExtractor, rules, ruleExecutor, callback, window, new Metrics(metrics), clock);
	}

	public WindowBuilder withWindow(Duration precision, RuleWindow rule) {
		Objects.requireNonNull(rule, "Window rule can not be null");
		Objects.requireNonNull(precision, "Precision can not be null");
		this.windowBuilder = new WindowBuilder(this, precision, rule);
		return this.windowBuilder;
	}

	public WindowBuilder withWindow(RuleWindow rule) {
		Objects.requireNonNull(rule, "Window rule can not be null");
		this.windowBuilder = new WindowBuilder(this, Duration.ofSeconds(10), rule);
		return this.windowBuilder;
	}

	public ShepherdBuilder threads(int thread) {
		if (thread < 1) {
			throw new IllegalArgumentException("threads must be bigger than 0");
		}
		this.thread = thread;
		return this;
	}

	public static class WindowBuilder<T> {

		private final ShepherdBuilder shepherdBuilder;

		private final RuleWindow rule;

		private final Duration precision;

		private Supplier<Scheduler> schedulerProvider;

		public WindowBuilder(ShepherdBuilder shepherdBuilder, Duration precision, RuleWindow ruleWindow) {
			Objects.requireNonNull(ruleWindow, "Rule applied to close the Window can not be null. Hint: if you don't need them, don't use the Window");
			Objects.requireNonNull(precision, "Precision can not be null");
			this.shepherdBuilder = shepherdBuilder;
			this.rule = ruleWindow;
			this.precision = precision;
		}

		public WindowBuilder setSchedulerProvider(Supplier<Scheduler> schedulerProvider) {
			Objects.requireNonNull(schedulerProvider, "SchedulerProvider can not be null. Hint: if you don't need it, just leave it empty");
			this.schedulerProvider = schedulerProvider;
			return this;
		}

		private Window buildWindow() {
			return rule == null ? null : new Window(rule, precision, schedulerProvider, clock);
		}


		public ShepherdASync build() {
			return this.shepherdBuilder.build(new Window(rule, precision, schedulerProvider, clock));
		}

		public ShepherdSync buildSync() {
			return this.shepherdBuilder.buildSync(new Window(rule, precision, schedulerProvider, clock));
		}
	}
}