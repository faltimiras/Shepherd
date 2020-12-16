package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.RuleWindow;
import cat.altimiras.shepherd.scheduler.BasicScheduler;
import cat.altimiras.shepherd.scheduler.Scheduler;
import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Window<T> {

	private static final Logger log = LoggerFactory.getLogger(Window.class);

	private final RuleWindow ruleWindow;

	private final Duration precision;

	private final Supplier<Scheduler> schedulerProvider;

	public Window(RuleWindow ruleWindow, Duration precision, Supplier<Scheduler> schedulerProvider, Clock clock) {
		this.ruleWindow = ruleWindow;
		this.precision = precision;
		if (schedulerProvider == null) {
			log.info("Not scheduler provided. BasicScheduler configured");
			this.schedulerProvider = () -> new BasicScheduler(clock, precision);
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
