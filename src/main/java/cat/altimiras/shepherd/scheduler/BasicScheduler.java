package cat.altimiras.shepherd.scheduler;

import cat.altimiras.shepherd.Metrics;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicScheduler implements Scheduler {

	private static final Logger log = LoggerFactory.getLogger(BasicScheduler.class);

	private final Clock clock;

	private final long maxTime;

	private boolean firstTime = true;

	private long lastExecution;

	public BasicScheduler(Clock clock, Duration maxTime) {
		this.clock = clock;
		this.maxTime = maxTime.toMillis();
		this.lastExecution = clock.millis();
	}

	@Override
	public long calculateWaitingTime(Metrics metrics) {
		if (firstTime) {
			firstTime = false;
			return maxTime;
		}
		long diff = clock.millis() - lastExecution;
		long wait = diff < maxTime ? maxTime - diff : 0;
		log.debug("Max time: {}ms. Diff since last execution: {}ms. So time to wait: {}", maxTime, diff, wait);
		return wait;
	}

	@Override
	public void justExecuted() {
		this.lastExecution = clock.millis();
	}
}