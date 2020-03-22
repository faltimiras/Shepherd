package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.RuleExecutor;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BasicConsumerTest {

	private String key = "key";
	private List values = Arrays.asList(1, 2, 3);
	private Consumer<List> callback = mock(Consumer.class);
	private RuleExecutor ruleExecutor = mock(RuleExecutor.class);

/*
	@Test
	public void consume2Rules() throws Exception {

		Metrics metrics = mock(Metrics.class);
		MetadataStorage metadataStorage = mock(MetadataStorage.class);
		ValuesStorage valuesStorage = mock(ValuesStorage.class);
		RuleExecutor ruleExecutor = mock(RuleExecutor.class);
		Consumer callback = mock(Consumer.class);
		BlockingQueue blockingQueue = new LinkedBlockingDeque();
		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);

		when(metadataStorage.get(key)).thenReturn(new Metadata(key, 0l));
		when()


		BasicConsumer basicConsumer = new BasicConsumer(metadataStorage, valuesStorage, rules, blockingQueue, ruleExecutor, callback, metrics);

		InputValue inputValue = new InputValue(1, key);
		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();
		verify(callback, times(1)).accept(values);
	}
/*
	@Test
	public void keepResultAfterGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutor.execute(any(Record.class), eq(rules))).thenReturn(RuleResult.canGroup(values, record));

		BasicConsumer basicConsumer = new BasicConsumer(rules, new LinkedBlockingDeque<>(), ruleExecutor, callback);
		basicConsumer.consume(record);


		verify(callback, times(1)).accept(values);
		assertEquals(values, basicConsumer.getOrElse(key).getValues());
	}

	@Test
	public void keepResultAfterNoGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutor.execute(any(Record.class), eq(rules))).thenReturn(RuleResult.cantGroup(record));

		BasicConsumer basicConsumer = new BasicConsumer(rules, new LinkedBlockingDeque<>(), ruleExecutor, callback);
		basicConsumer.consume(record);


		verify(callback, never()).accept(any());
		assertEquals(values, basicConsumer.getOrElse(key).getValues());
	}
*/
}
