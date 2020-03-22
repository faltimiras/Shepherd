package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlidingWindowRuleTest {

	@Test
	public void expiredCreation() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(113l);

		GroupAllExpiredRule groupAllExpiredRule = new GroupAllExpiredRule(Duration.ofMillis(100l), clock, false);

		Metadata metadata = new Metadata("key", 12);
		RuleResult ruleResult = groupAllExpiredRule.canClose(metadata, null);

		assertTrue(ruleResult.canClose());
	}

	@Test
	public void notExpiredCreation() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(110l);

		GroupAllExpiredRule groupAllExpiredRule = new GroupAllExpiredRule(Duration.ofMillis(100l), clock, false);

		Metadata metadata = new Metadata("key", 12);
		RuleResult ruleResult = groupAllExpiredRule.canClose(metadata, null);

		assertFalse(ruleResult.canClose());
	}

	@Test
	public void expiredLatest() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(113l);

		GroupAllExpiredRule groupAllExpiredRule = new GroupAllExpiredRule(Duration.ofMillis(100l), clock, true);

		Metadata metadata = new Metadata("key", 12000l);
		metadata.setLastElementTs(12l);
		RuleResult ruleResult = groupAllExpiredRule.canClose(metadata, null);

		assertTrue(ruleResult.canClose());
	}

	@Test
	public void notExpiredLatest() throws Exception {

		Clock clock = mock(Clock.class);
		when(clock.millis()).thenReturn(110l);

		GroupAllExpiredRule groupAllExpiredRule = new GroupAllExpiredRule(Duration.ofMillis(100l), clock, true);

		Metadata metadata = new Metadata("key", 120000l);
		metadata.setLastElementTs(12l);
		RuleResult ruleResult = groupAllExpiredRule.canClose(metadata, null);

		assertFalse(ruleResult.canClose());
	}
}
