package cat.altimiras.shepherd.rules.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.RuleWindow;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class TumblingWindowRuleTest {

	private final long eventTs = 1584612805000L;  //Thu Mar 19 2020 10:13:25

	private final Clock clock = mock(Clock.class);

	@Test
	public void expired() throws Exception {

		when(clock.millis())
				.thenReturn(1584612804900L) //metadata creation
				.thenReturn(1584612950300L);//when validation

		GroupExpiredTumblingWindowRule tumblingWindowRule = new GroupExpiredTumblingWindowRule(Duration.ofMinutes(2), Duration.ofMillis(0), clock);

		RuleWindow.WindowKey windowKey = tumblingWindowRule.adaptKey("k", eventTs); // //Thu Mar 19 2020 11:13:34
		assertEquals(1584612840000L, windowKey.getWindow()); //Thu Mar 19 2020 10:14:00

		Metadata metadata = new Metadata(windowKey, eventTs, clock);
		assertTrue(tumblingWindowRule.isWindowExpired(metadata));//Thu Mar 19 2020 11:14:00
	}

	@Test
	public void within() throws Exception {

		when(clock.millis())
				.thenReturn(1584612804900L) //metadata creation
				.thenReturn(1584612830300L);//when validation

		GroupExpiredTumblingWindowRule tumblingWindowRule = new GroupExpiredTumblingWindowRule(Duration.ofMinutes(2), Duration.ofMillis(0), clock);

		RuleWindow.WindowKey windowKey = tumblingWindowRule.adaptKey("k", eventTs); // //Thu Mar 19 2020 11:13:34
		assertEquals(1584612840000L, windowKey.getWindow()); //Thu Mar 19 2020 10:14:00

		Metadata metadata = new Metadata(windowKey, eventTs, clock);
		assertFalse(tumblingWindowRule.isWindowExpired(metadata));//Thu Mar 19 2020 11:14:00
	}
}
