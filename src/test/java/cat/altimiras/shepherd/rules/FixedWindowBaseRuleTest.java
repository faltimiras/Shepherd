package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FixedWindowBaseRuleTest {

	@Test
	public void niceMinute() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(1584616414116l); //Thu Mar 19 2020 11:13:34

		FixedWindowRule dummyFixedWindowRule = new FixedWindowRule(Duration.ofMinutes(5), clock);

		long end = dummyFixedWindowRule.getEndCurrentOpenWindow();
		assertEquals(1584616500000l, end); //Thu Mar 19 2020 11:15:00
	}

	@Test
	public void oclockMinute() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(1584616500000l);  //Thu Mar 19 2020 11:15:00

		FixedWindowRule dummyFixedWindowRule = new FixedWindowRule(Duration.ofMinutes(5), clock);

		long end = dummyFixedWindowRule.getEndCurrentOpenWindow();
		assertEquals(1584616800000l, end); //Thu Mar 19 2020 11:12:00
	}

	@Test
	public void niceMilli() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(1584617575123l);  //Thu Mar 19 2020 12:32:55:123

		FixedWindowRule dummyFixedWindowRule = new FixedWindowRule(Duration.ofMillis(50), clock);

		long end = dummyFixedWindowRule.getEndCurrentOpenWindow();
		assertEquals(1584617575150l, end); //Thu Mar 19 2020 12:32:55:150
	}

	@Test
	public void niceHour() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(1584617575123l);  //Thu Mar 19 2020 12:32:55:123

		FixedWindowRule dummyFixedWindowRule = new FixedWindowRule(Duration.ofHours(2), clock);

		long end = dummyFixedWindowRule.getEndCurrentOpenWindow();
		assertEquals(1584619200000l, end); //Thu Mar 19 2020 12:00:00
	}

	private class FixedWindowRule extends FixedWindowBaseRule {

		public FixedWindowRule(Duration window, Clock clock) {
			super(window, clock);
		}

		@Override
		public RuleResult canGroup(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {
			return null;
		}
	}
}
