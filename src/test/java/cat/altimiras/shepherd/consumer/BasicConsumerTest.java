package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.Callback;
import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BasicConsumerTest {

	private String key = "key";
	private List values = Arrays.asList(1, 2, 3);
	private Element element = new Element(key, values);
	private Callback callback = mock(Callback.class);
	private RuleExecutor ruleExecutor = mock(RuleExecutor.class);

	@Test
	public void storage() throws Exception {

		BasicConsumer basicConsumer = new BasicConsumer(null, null, null, null);

		Element test = new Element(key, Arrays.asList(1, 2, 3));

		basicConsumer.put(test);
		assertEquals(test, basicConsumer.getOrElse(key));
		assertEquals(3, basicConsumer.getOrElse(key).getValues().size());

		basicConsumer.remove(key);
		assertNotEquals(test, basicConsumer.getOrElse(key));
		assertTrue(basicConsumer.getOrElse(key).getValues().isEmpty());
	}

	@Test
	public void consume2Rules() throws Exception {

		Rule rule1 = mock(Rule.class);
		Rule rule2 = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule1, rule2);
		when(ruleExecutor.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values));


		BasicConsumer basicConsumer = new BasicConsumer(rules, new LinkedBlockingDeque<>(), ruleExecutor, callback);
		basicConsumer.consume(element);


		verify(callback, times(1)).accept(values);
	}

	@Test
	public void keepResultAfterGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutor.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values, element));

		BasicConsumer basicConsumer = new BasicConsumer(rules, new LinkedBlockingDeque<>(), ruleExecutor, callback);
		basicConsumer.consume(element);


		verify(callback, times(1)).accept(values);
		assertEquals(values, basicConsumer.getOrElse(key).getValues());
	}

	@Test
	public void keepResultAfterNoGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutor.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.cantGroup(element));

		BasicConsumer basicConsumer = new BasicConsumer(rules, new LinkedBlockingDeque<>(), ruleExecutor, callback);
		basicConsumer.consume(element);


		verify(callback, never()).accept(any());
		assertEquals(values, basicConsumer.getOrElse(key).getValues());
	}

}
