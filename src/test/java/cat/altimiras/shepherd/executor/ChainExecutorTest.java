package cat.altimiras.shepherd.executor;

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
/*
	private final String key = "key";
	private Record record = new Record(key);
	private List values = Arrays.asList(1, 2, 3);
	private Record record2 = new Record(key, Arrays.asList(4,5,6));

	@Test
	public void firstCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.canGroup(values));
		Rule rule2 = mock(Rule.class);

		ChainExecutor chainExecutor = new ChainExecutor();

		RuleResult ruleResult = chainExecutor.execute(record, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canGroup());
		assertEquals(values, ruleResult.getGroup());
		verify(rule1, times(1)).canGroup(record);
		verify(rule2, never()).canGroup(any());
	}

	@Test(expected = NullPointerException.class)
	public void firstDontReturnAnything() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.cantGroup());
		Rule rule2 = mock(Rule.class);

		ChainExecutor chainExecutor = new ChainExecutor();

		chainExecutor.execute(record, Arrays.asList(rule1, rule2));

	}

	@Test
	public void secondCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.cantGroup(record2));
		Rule rule2 = mock(Rule.class);
		when(rule2.canGroup(record2)).thenReturn(RuleResult.canGroup(values));

		ChainExecutor chainExecutor = new ChainExecutor();

		RuleResult ruleResult = chainExecutor.execute(record, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canGroup());
		assertEquals(values, ruleResult.getGroup());
		verify(rule1, times(1)).canGroup(record);
		verify(rule2, times(1)).canGroup(record2);
	}

	@Test
	public void cantGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canGroup(record)).thenReturn(RuleResult.cantGroup(record2));
		Rule rule2 = mock(Rule.class);
		when(rule2.canGroup(record2)).thenReturn(RuleResult.cantGroup(record));

		ChainExecutor chainExecutor = new ChainExecutor();

		RuleResult ruleResult = chainExecutor.execute(record, Arrays.asList(rule1, rule2));

		assertFalse(ruleResult.canGroup());
		assertEquals(record, ruleResult.getToKeep());
		verify(rule1, times(1)).canGroup(record);
		verify(rule2, times(1)).canGroup(record2);
	}
*/
}
