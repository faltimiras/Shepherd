package cat.altimiras.shepherd.executor;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChainExecutorTest {

	private final String key = "key";
	private Element element = new Element(key);
	private List values = Arrays.asList(1, 2, 3);
	private Element element2 = new Element(key, Arrays.asList(4,5,6));

	@Test
	public void firstCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(element)).thenReturn(RuleResult.canGroup(values));
		Rule rule2 = mock(Rule.class);

		ChainExecutor chainExecutor = new ChainExecutor();

		RuleResult ruleResult = chainExecutor.execute(element, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canGroup());
		assertEquals(values, ruleResult.getGroup());
		verify(rule1, times(1)).canGroup(element);
		verify(rule2, never()).canGroup(any());
	}

	@Test(expected = NullPointerException.class)
	public void firstDontReturnAnything() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(element)).thenReturn(RuleResult.cantGroup());
		Rule rule2 = mock(Rule.class);

		ChainExecutor chainExecutor = new ChainExecutor();

		chainExecutor.execute(element, Arrays.asList(rule1, rule2));

	}

	@Test
	public void secondCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(element)).thenReturn(RuleResult.cantGroup(element2));
		Rule rule2 = mock(Rule.class);
		when(rule2.canGroup(element2)).thenReturn(RuleResult.canGroup(values));

		ChainExecutor chainExecutor = new ChainExecutor();

		RuleResult ruleResult = chainExecutor.execute(element, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canGroup());
		assertEquals(values, ruleResult.getGroup());
		verify(rule1, times(1)).canGroup(element);
		verify(rule2, times(1)).canGroup(element2);
	}

	@Test
	public void cantGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(element)).thenReturn(RuleResult.cantGroup(element2));
		Rule rule2 = mock(Rule.class);
		when(rule2.canGroup(element2)).thenReturn(RuleResult.cantGroup(element));

		ChainExecutor chainExecutor = new ChainExecutor();

		RuleResult ruleResult = chainExecutor.execute(element, Arrays.asList(rule1, rule2));

		assertFalse(ruleResult.canGroup());
		assertEquals(element, ruleResult.getToKeep());
		verify(rule1, times(1)).canGroup(element);
		verify(rule2, times(1)).canGroup(element2);
	}

}
