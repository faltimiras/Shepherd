package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.RuleWindow;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TumblingWindowRuleTest {

	private long eventTs= 1584612805000L;  //Thu Mar 19 2020 10:13:25
	private Clock clock = Mockito.mock(Clock.class);

	@Test
	public void expired() throws Exception {

		when(clock.millis()).thenReturn(1584612840000L);//Thu Mar 19 2020 10:14:00

		GroupExpiredTumblingWindowRule tumblingWindowRule = new GroupExpiredTumblingWindowRule(Duration.ofMinutes(2), Duration.ofMillis(0), clock);

		RuleWindow.WindowKey windowKey = tumblingWindowRule.adaptKey("k", eventTs); // //Thu Mar 19 2020 11:13:34
		assertEquals(1584612840000L, windowKey.getWindow()); //Thu Mar 19 2020 10:14:00

		Metadata metadata = new Metadata(windowKey,eventTs);
		assertTrue(tumblingWindowRule.isWindowExpired(metadata));//Thu Mar 19 2020 11:14:00
	}

	@Test
	public void within() throws Exception {

		when(clock.millis()).thenReturn(1584612830000L);//Thu Mar 19 2020 10:13:50

		GroupExpiredTumblingWindowRule tumblingWindowRule = new GroupExpiredTumblingWindowRule(Duration.ofMinutes(2), Duration.ofMillis(0), clock);

		RuleWindow.WindowKey windowKey = tumblingWindowRule.adaptKey("k", eventTs); // //Thu Mar 19 2020 11:13:34
		assertEquals(1584612840000L, windowKey.getWindow()); //Thu Mar 19 2020 10:14:00

		Metadata metadata = new Metadata(windowKey,eventTs);
		assertFalse(tumblingWindowRule.isWindowExpired(metadata));//Thu Mar 19 2020 11:14:00
	}
}
