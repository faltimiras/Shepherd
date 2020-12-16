package cat.altimiras.shepherd.rules.window;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public class SlidingWindowRuleTest {

	@Test
	public void expiredCreation() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(12l)
				.thenReturn(113l);

		GroupExpiredSlidingRule groupExpiredSlidingRule = new GroupExpiredSlidingRule(Duration.ofMillis(100l), false, clock);

		Metadata metadata = new Metadata("key", 12, clock);
		RuleResult ruleResult = groupExpiredSlidingRule.canClose(metadata, null);

		assertTrue(ruleResult.canClose());
	}

	@Test
	public void notExpiredCreation() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(12l)
				.thenReturn(110l);

		GroupExpiredSlidingRule groupExpiredSlidingRule = new GroupExpiredSlidingRule(Duration.ofMillis(100l), false, clock);

		Metadata metadata = new Metadata("key", 12, clock);
		RuleResult ruleResult = groupExpiredSlidingRule.canClose(metadata, null);

		assertFalse(ruleResult.canClose());
	}

	@Test
	public void expiredLatest() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(1l)
				.thenReturn(12l)
				.thenReturn(122l);

		GroupExpiredSlidingRule groupExpiredSlidingRule = new GroupExpiredSlidingRule(Duration.ofMillis(100l), true, clock);

		Metadata metadata = new Metadata("key", 12000l, clock);
		metadata.setLastElementTs(12l);

		RuleResult ruleResult = groupExpiredSlidingRule.canClose(metadata, null);

		assertTrue(ruleResult.canClose());
	}

	@Test
	public void notExpiredLatest() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(100l)
				.thenReturn(110l);

		GroupExpiredSlidingRule groupExpiredSlidingRule = new GroupExpiredSlidingRule(Duration.ofMillis(100l), true, clock);

		Metadata metadata = new Metadata("key", 120000l, clock);
		metadata.setLastElementTs(12l);
		RuleResult ruleResult = groupExpiredSlidingRule.canClose(metadata, null);

		assertFalse(ruleResult.canClose());
	}

	@Test
	public void oldValue() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis())
				.thenReturn(15l)
				.thenReturn(110l);

		GroupExpiredSlidingRule groupExpiredSlidingRule = new GroupExpiredSlidingRule(Duration.ofMillis(100l), true, clock);

		Metadata metadata = new Metadata("key", 120000l, clock);
		metadata.setLastElementTs(12l);
		RuleResult ruleResult = groupExpiredSlidingRule.canClose(metadata, null);

		assertFalse(ruleResult.canClose());
	}
}
