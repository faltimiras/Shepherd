package cat.altimiras.shepherd.scheduler;

import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasicSchedulerTest {

	@Test
	public void shouldRun()throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.instant())
				.thenReturn(Instant.ofEpochMilli(0l)) //set last execution
				.thenReturn(Instant.ofEpochMilli(20l)); //moment to check

		BasicScheduler basicScheduler = new BasicScheduler(clock, Duration.ofMillis(10));

		long t = basicScheduler.calculateWaitingTime();
		assertEquals(0, t);
	}

	@Test
	public void shouldNotRun()throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.instant())
				.thenReturn(Instant.ofEpochMilli(0l)) //set last execution
				.thenReturn(Instant.ofEpochMilli(5l)); //moment to check

		BasicScheduler basicScheduler = new BasicScheduler(clock, Duration.ofMillis(10));

		long t = basicScheduler.calculateWaitingTime();
		assertEquals(5, t);
	}

}
