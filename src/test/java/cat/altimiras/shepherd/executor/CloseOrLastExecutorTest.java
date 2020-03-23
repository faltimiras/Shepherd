package cat.altimiras.shepherd.executor;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloseOrLastExecutorTest {

	private final String key = "key";
	private final Object object = new Object();
	private final LazyValue lazyValue = new LazyValue(null, key);
	private Metadata metadata = new Metadata(key, 0l);

	@Test
	public void firstCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canClose(metadata, object,  lazyValue)).thenReturn(RuleResult.groupAndDiscardAll());
		Rule rule2 = mock(Rule.class);

		CloseOrLastExecutor closeOrLastExecutor = new CloseOrLastExecutor();

		RuleResult ruleResult = closeOrLastExecutor.execute(metadata, object, lazyValue, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canClose());
		assertNull(ruleResult.getGroup());
		assertEquals(1,ruleResult.getDiscard());
		verify(rule1, times(1)).canClose(metadata, object, lazyValue);
		verify(rule2, never()).canClose(any(Metadata.class), any(Object.class), any(LazyValue.class));
	}

	@Test
	public void secondCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canClose(metadata, object, lazyValue)).thenReturn(RuleResult.notGroup());
		Rule rule2 = mock(Rule.class);
		when(rule2.canClose(metadata, object, lazyValue)).thenReturn(RuleResult.groupAndDiscardAll());

		CloseOrLastExecutor closeOrLastExecutor = new CloseOrLastExecutor();

		RuleResult ruleResult = closeOrLastExecutor.execute(metadata, object, lazyValue, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canClose());
		assertNull(ruleResult.getGroup());
		assertEquals(1,ruleResult.getDiscard());
		verify(rule1, times(1)).canClose(metadata, object, lazyValue);
		verify(rule2, times(1)).canClose(metadata, object, lazyValue);
	}

	@Test
	public void cantGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canClose(metadata, object, lazyValue)).thenReturn(RuleResult.notGroup());
		Rule rule2 = mock(Rule.class);
		when(rule2.canClose(metadata, object, lazyValue)).thenReturn(RuleResult.notGroup());

		CloseOrLastExecutor closeOrLastExecutor = new CloseOrLastExecutor();

		RuleResult ruleResult = closeOrLastExecutor.execute(metadata, object, lazyValue, Arrays.asList(rule1, rule2));

		assertFalse(ruleResult.canClose());
		assertNull(ruleResult.getGroup());
		assertEquals(0,ruleResult.getDiscard());
		verify(rule1, times(1)).canClose(metadata, object, lazyValue);
		verify(rule2, times(1)).canClose(metadata, object, lazyValue);
	}
}
