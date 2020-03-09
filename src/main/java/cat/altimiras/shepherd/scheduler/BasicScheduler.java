package cat.altimiras.shepherd.scheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public class BasicScheduler implements Scheduler {

	private final Clock clock;
	private final Duration maxTime;

	private Instant lastExecution;

	public BasicScheduler(Clock clock, Duration maxTime) {
		this.clock = clock;
		this.maxTime = maxTime;
		this.lastExecution = clock.instant();
	}

	@Override
	public long calculateWaitingTime() {
		Duration diff = Duration.between(lastExecution, clock.instant());
		return diff.compareTo(maxTime) < 0 ? maxTime.minus(diff).toMillis() : 0;
	}

	@Override
	public void justExecuted() {
		this.lastExecution = clock.instant();
	}
}