package cat.altimiras.shepherd.executor;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IndependentExecutorTest {
/*
	private final String key = "key";
	private Record record = new Record(key);
	private List values = Arrays.asList(1, 2, 3);

	@Test
	public void firstCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.canGroup(values));
		Rule rule2 = mock(Rule.class);

		IndependentExecutor independentExecutor = new IndependentExecutor();

		RuleResult ruleResult = independentExecutor.execute(record, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canGroup());
		assertEquals(values, ruleResult.getGroup());
		verify(rule1, times(1)).canGroup(record);
		verify(rule2, never()).canGroup(any());
	}

	@Test
	public void secondCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.cantGroup());
		Rule rule2 = mock(Rule.class);
		when(rule2.canGroup(record)).thenReturn(RuleResult.canGroup(values));

		IndependentExecutor independentExecutor = new IndependentExecutor();

		RuleResult ruleResult = independentExecutor.execute(record, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canGroup());
		assertEquals(values, ruleResult.getGroup());
		verify(rule1, times(1)).canGroup(record);
		verify(rule2, times(1)).canGroup(record);
	}

	@Test
	public void cantGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.cantGroup());
		Rule rule2 = mock(Rule.class);
		when(rule2.canGroup(record)).thenReturn(RuleResult.cantGroup(record));

		IndependentExecutor independentExecutor = new IndependentExecutor();

		RuleResult ruleResult = independentExecutor.execute(record, Arrays.asList(rule1, rule2));

		assertFalse(ruleResult.canGroup());
		assertEquals(record, ruleResult.getToKeep());
		verify(rule1, times(1)).canGroup(record);
		verify(rule2, times(1)).canGroup(record);
	}


 */
}
