package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class AccumulateRuleTest {

	@Test
	public void firstElement() throws Exception {

		AccumulateNRule accumulateNRule = new AccumulateNRule(2);

		Element element = new Element("key", 1);

		RuleResult ruleResult = accumulateNRule.canGroup(element);

		assertFalse(ruleResult.canGroup());
		assertEquals(element, ruleResult.getToKeep());
	}

	@Test
	public void accumulateSimple() throws Exception {

		AccumulateNRule accumulateNRule = new AccumulateNRule(2);

		Element element = new Element("key", Arrays.asList("a","c"));

		RuleResult ruleResult = accumulateNRule.canGroup(element);

		assertTrue(ruleResult.canGroup());
		assertEquals(2, ruleResult.getGroup().size());
		assertEquals("a", ruleResult.getGroup().get(0));
		assertEquals("c", ruleResult.getGroup().get(1));
	}
}
