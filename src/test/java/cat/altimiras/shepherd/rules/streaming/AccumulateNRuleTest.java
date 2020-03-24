package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AccumulateNRuleTest {

	@Test
	public void first(){

		AccumulateNRule accumulateNRule = new AccumulateNRule(3);

		Metadata metadata = new Metadata("k", 0l);
		RuleResult ruleResult = accumulateNRule.canClose(metadata, new Object(), null);

		assertFalse(ruleResult.canClose());
		assertEquals(1, ruleResult.getAppend());
		assertEquals(0, ruleResult.getDiscard());
	}

	@Test
	public void accumulate(){

		AccumulateNRule accumulateNRule = new AccumulateNRule(2);

		Metadata metadata = new Metadata("k", 0l);
		metadata.setElementsCount(1);
		RuleResult ruleResult = accumulateNRule.canClose(metadata, new Object(), null);

		assertTrue(ruleResult.canClose());
		assertEquals(-1, ruleResult.getAppend());
		assertEquals(1, ruleResult.getDiscard());
	}

	@Test
	public void noAccumulate(){

		AccumulateNRule accumulateNRule = new AccumulateNRule(3);

		Metadata metadata = new Metadata("k", 0l);
		metadata.setElementsCount(1);
		RuleResult ruleResult = accumulateNRule.canClose(metadata, new Object(), null);

		assertFalse(ruleResult.canClose());
		assertEquals(1, ruleResult.getAppend());
		assertEquals(0, ruleResult.getDiscard());
	}
}
