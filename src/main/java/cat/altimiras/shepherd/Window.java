package cat.altimiras.shepherd;

import cat.altimiras.shepherd.scheduler.BasicScheduler;
import cat.altimiras.shepherd.scheduler.Scheduler;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;

public class Window<T> {
	private RuleWindow ruleWindow;
	private Duration precision;
	private Supplier<Scheduler> schedulerProvider;

	public Window(RuleWindow ruleWindow, Duration precision, Supplier<Scheduler> schedulerProvider) {
		this.ruleWindow = ruleWindow;
		this.precision = precision;
		if (schedulerProvider == null) {
			this.schedulerProvider = () -> new BasicScheduler(Clock.systemUTC(), precision);
		} else {
			this.schedulerProvider = schedulerProvider;
		}
	}

	public RuleWindow getRule() {
		return ruleWindow;
	}

	public Duration getPrecision() {
		return precision;
	}

	public Supplier<Scheduler> getSchedulerProvider() {
		return schedulerProvider;
	}
}
