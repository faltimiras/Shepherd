package cat.altimiras.shepherd.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import java.time.Clock;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class CloseOrLastExecutorTest {

	private final String key = "key";

	private final Object object = new Object();

	private final LazyValues lazyValues = new LazyValues(null, key);

	private final Metadata metadata = new Metadata(key, 0l, Clock.systemUTC());

	@Test
	public void firstCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canClose(metadata, object, lazyValues)).thenReturn(RuleResult.groupAndDiscardAll());
		Rule rule2 = mock(Rule.class);

		CloseOrLastExecutor closeOrLastExecutor = new CloseOrLastExecutor();

		RuleResult ruleResult = closeOrLastExecutor.execute(metadata, object, lazyValues, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canClose());
		assertNull(ruleResult.getGroup());
		assertEquals(1, ruleResult.getDiscard());
		verify(rule1, times(1)).canClose(metadata, object, lazyValues);
		verify(rule2, never()).canClose(any(Metadata.class), any(Object.class), any(LazyValues.class));
	}

	@Test
	public void secondCanGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canClose(metadata, object, lazyValues)).thenReturn(RuleResult.notGroup());
		Rule rule2 = mock(Rule.class);
		when(rule2.canClose(metadata, object, lazyValues)).thenReturn(RuleResult.groupAndDiscardAll());

		CloseOrLastExecutor closeOrLastExecutor = new CloseOrLastExecutor();

		RuleResult ruleResult = closeOrLastExecutor.execute(metadata, object, lazyValues, Arrays.asList(rule1, rule2));

		assertTrue(ruleResult.canClose());
		assertNull(ruleResult.getGroup());
		assertEquals(1, ruleResult.getDiscard());
		verify(rule1, times(1)).canClose(metadata, object, lazyValues);
		verify(rule2, times(1)).canClose(metadata, object, lazyValues);
	}

	@Test
	public void cantGroup() throws Exception {

		Rule rule1 = mock(Rule.class);
		when(rule1.canClose(metadata, object, lazyValues)).thenReturn(RuleResult.notGroup());
		Rule rule2 = mock(Rule.class);
		when(rule2.canClose(metadata, object, lazyValues)).thenReturn(RuleResult.notGroup());

		CloseOrLastExecutor closeOrLastExecutor = new CloseOrLastExecutor();

		RuleResult ruleResult = closeOrLastExecutor.execute(metadata, object, lazyValues, Arrays.asList(rule1, rule2));

		assertFalse(ruleResult.canClose());
		assertNull(ruleResult.getGroup());
		assertEquals(0, ruleResult.getDiscard());
		verify(rule1, times(1)).canClose(metadata, object, lazyValues);
		verify(rule2, times(1)).canClose(metadata, object, lazyValues);
	}
}
