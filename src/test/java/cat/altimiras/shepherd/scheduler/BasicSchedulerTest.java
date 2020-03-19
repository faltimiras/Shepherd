package cat.altimiras.shepherd.scheduler;

import org.junit.Test;

import java.time.Clock;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasicSchedulerTest {

	@Test
	public void shouldRun() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(0l) //set last execution
				.thenReturn(20l);//moment to check

		BasicScheduler basicScheduler = new BasicScheduler(clock, Duration.ofMillis(10));

		long t = basicScheduler.calculateWaitingTime(0l);
		assertEquals(10, t); //first execution is always the precision  time
		long t2 = basicScheduler.calculateWaitingTime(0l);
		assertEquals(0, t2); //first execution is always the precision  time
	}

	@Test
	public void shouldNotRun() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(0l) //set last execution
				.thenReturn(5l);//moment to check


		BasicScheduler basicScheduler = new BasicScheduler(clock, Duration.ofMillis(10));

		long t = basicScheduler.calculateWaitingTime(0l);
		assertEquals(10, t); //first execution is always the precision  time
		long t2 = basicScheduler.calculateWaitingTime(0l);
		assertEquals(5, t2);
	}
}
