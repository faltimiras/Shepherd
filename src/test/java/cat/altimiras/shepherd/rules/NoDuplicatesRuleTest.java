package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class NoDuplicatesRuleTest {

	@Test
	public void firstElement() throws Exception {

		NoDuplicatesRule noDuplicatesRule = new NoDuplicatesRule();

		Element element = new Element("key", 1);

		RuleResult ruleResult = noDuplicatesRule.canGroup(element);

		assertTrue(ruleResult.canGroup());
		assertEquals(element, ruleResult.getToKeep());
		assertEquals(1, ruleResult.getGroup().get(0));
	}

	@Test
	public void repeatedElement() throws Exception {

		NoDuplicatesRule noDuplicatesRule = new NoDuplicatesRule();

		Element element = new Element("key", Arrays.asList(1,1));

		RuleResult ruleResult = noDuplicatesRule.canGroup(element);

		assertFalse(ruleResult.canGroup());
		assertEquals(1, ruleResult.getToKeep().getValues().get(0));
	}

	@Test
	public void noRepeatedElement() throws Exception {

		NoDuplicatesRule noDuplicatesRule = new NoDuplicatesRule();

		Element element = new Element("key", Arrays.asList(1,2));

		RuleResult ruleResult = noDuplicatesRule.canGroup(element);

		assertTrue(ruleResult.canGroup());
		assertEquals(2, ruleResult.getToKeep().getValues().get(0));
		assertEquals(2, ruleResult.getGroup().get(0));
	}
}