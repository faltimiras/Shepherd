package cat.altimiras.shepherd.scheduler;

import cat.altimiras.shepherd.Metrics;

import java.time.Clock;
import java.time.Duration;

public class BasicScheduler implements Scheduler {

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
		return diff < maxTime ? maxTime - diff : 0;
	}

	@Override
	public void justExecuted() {
		this.lastExecution = clock.millis();
	}
}