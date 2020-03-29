package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.InputValue;
import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Metrics;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.storage.MetadataStorage;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class BasicConsumerTest {

	private String key = "key";
	private Metadata metadata = new Metadata(key, 123L);
	private Object object = new Object();
	private List get = Collections.singletonList(new Object());
	private InputValue inputValue = new InputValue(object, key, 123L);

	private Consumer<List> callback;
	private RuleExecutor ruleExecutor;
	private Metrics metrics;
	private MetadataStorage metadataStorage;
	private ValuesStorage valuesStorage;
	private List<Rule> rules;

	private BasicConsumer basicConsumer;

	@Before
	public void setUp() {
		callback = mock(Consumer.class);
		ruleExecutor = mock(RuleExecutor.class);
		metrics = mock(Metrics.class);
		metadataStorage = mock(MetadataStorage.class);
		valuesStorage = mock(ValuesStorage.class);
		BlockingQueue blockingQueue = new LinkedBlockingDeque();
		Rule rule = mock(Rule.class);
		rules = Collections.singletonList(rule);

		basicConsumer = new BasicConsumer(metadataStorage, valuesStorage, rules, blockingQueue, ruleExecutor, callback, metrics);
	}

	@Test
	public void firstNoGroup() {

		when(metadataStorage.get(key)).thenReturn(null);
		when(ruleExecutor.execute(any(Metadata.class), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.notGroup());

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, times(1)).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(any());
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, never()).get(any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, never()).accept(any());
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void secondNoGroup() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.notGroup());

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(any());
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, never()).get(any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, never()).accept(any());
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void groupAndDiscardAll() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.groupAndDiscardAll());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, times(1)).remove(key);
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, times(1)).accept(get);
		verify(metadataStorage, times(1)).remove(any());
	}

	@Test
	public void groupAndAppend() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.groupAndAppend());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(key);
		verify(valuesStorage, times(1)).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, times(1)).accept(get);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void discardAndAppendAndGroup() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.discardAndAppendAndGroup());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, times(1)).remove(key);
		verify(valuesStorage, times(1)).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, times(1)).accept(get);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void appendAndGroup() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.appendAndGroup());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(key);
		verify(valuesStorage, times(1)).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, times(1)).accept(get);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void appendAndGroupAndDiscard() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.appendAndGroupAndDiscard());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, times(1)).remove(key);
		verify(valuesStorage, times(1)).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, times(1)).accept(get);
		verify(metadataStorage, times(1)).remove(any());
	}

	@Test
	public void groupAndDiscardAndAppend() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.groupAndDiscardAndAppend());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, times(1)).remove(key);
		verify(valuesStorage, times(1)).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, times(1)).accept(get);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void notGroupAndAppend() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.notGroupAndAppend());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(key);
		verify(valuesStorage, times(1)).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, never()).accept(get);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void notGroup() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.notGroup());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(key);
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, never()).accept(get);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void notGroupAndDiscardAll() {

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.notGroupAndDiscardAll());
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, times(1)).remove(key);
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(callback, never()).accept(get);
		verify(metadataStorage, times(1)).remove(any());
	}

	@Test
	public void groupAndKeep() {

		List<Integer> toGroup = Arrays.asList(1, 2, 3);
		List<Integer> toKeep = Arrays.asList(4, 5, 6);

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.groupAndKeep(toGroup, toKeep));
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(key);
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, times(1)).override(key, toKeep);
		verify(valuesStorage, never()).get(any());
		verify(callback, times(1)).accept(toGroup);
		verify(metadataStorage, never()).remove(any());
	}

	@Test
	public void groupAndDiscard() {

		List<Integer> toGroup = Arrays.asList(1, 2, 3);

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.groupAndDiscard(toGroup));
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, times(1)).remove(key);
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, never()).override(any(), any());
		verify(valuesStorage, never()).get(any());
		verify(callback, times(1)).accept(toGroup);
		verify(metadataStorage, times(1)).remove(any());
	}

	@Test
	public void notGroupAndKeep() {

		List<Integer> toKeep = Arrays.asList(1, 2, 3);

		when(metadataStorage.get(key)).thenReturn(metadata);
		when(ruleExecutor.execute(eq(metadata), eq(object), any(LazyValues.class), eq(rules))).thenReturn(RuleResult.notGroupAndKeep(toKeep));
		when(valuesStorage.get(key)).thenReturn(get);

		basicConsumer.consume(inputValue);

		verify(metrics, times(1)).rulesExecTime();
		verify(metrics, times(1)).pendingDec();

		verify(metadataStorage, never()).put(eq(key), any(Metadata.class));
		verify(valuesStorage, never()).remove(key);
		verify(valuesStorage, never()).append(any(), any());
		verify(valuesStorage, times(1)).override(key, toKeep);
		verify(valuesStorage, never()).get(any());
		verify(callback, never()).accept(any());
		verify(metadataStorage, never()).remove(any());
	}
}