package cat.altimiras.shepherd.consumer;

import cat.altimiras.shepherd.Callback;
import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.scheduler.Scheduler;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DogConsumerTest {

	private String key = "key";
	private List values = Arrays.asList(1, 2, 3);
	private Element element = new Element(key, values, 0l);
	private Callback callback = mock(Callback.class);
	private Scheduler scheduler = mock(Scheduler.class);
	private RuleExecutor ruleExecutor = mock(RuleExecutor.class);
	private RuleExecutor ruleExecutorTimeout = mock(RuleExecutor.class);

	@Before
	public void setUp() throws Exception {
		when(scheduler.calculateWaitingTime()).thenReturn(0l);
	}

	@Test
	public void storage() throws Exception {

		DogConsumer dogConsumer = new DogConsumer(null, null, null, null, null, null, null, null, null);

		Element test = new Element(key, Arrays.asList(1, 2, 3));

		dogConsumer.put(test);
		assertEquals(test, dogConsumer.getOrElse(key));
		assertEquals(3, dogConsumer.getOrElse(key).getValues().size());

		dogConsumer.remove(key);
		assertNotEquals(test, dogConsumer.getOrElse(key));
		assertTrue(dogConsumer.getOrElse(key).getValues().isEmpty());
	}

	@Test
	public void consume2Rules() throws Exception {

		Rule rule1 = mock(Rule.class);
		Rule rule2 = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule1, rule2);
		when(ruleExecutor.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values));

		DogConsumer dogConsumer = new DogConsumer(rules, ruleExecutor, new LinkedBlockingDeque<>(), scheduler, null, null, null, null, callback);
		dogConsumer.consume(element);

		verify(callback, times(1)).accept(values);
		verify(scheduler, never()).justExecuted();
	}

	@Test
	public void keepResultAfterGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutor.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values, element));

		DogConsumer dogConsumer = new DogConsumer(Arrays.asList(rule), ruleExecutor, new LinkedBlockingDeque<>(), scheduler, null, null, null, null, callback);
		dogConsumer.consume(element);

		verify(callback, times(1)).accept(values);
		assertEquals(values, dogConsumer.getOrElse(key).getValues());
		verify(scheduler, never()).justExecuted();
	}

	@Test
	public void keepResultAfterNoGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutor.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.cantGroup(element));

		DogConsumer dogConsumer = new DogConsumer(rules, ruleExecutor, new LinkedBlockingDeque<>(), scheduler, null, null, null, null, callback);
		dogConsumer.consume(element);

		verify(callback, never()).accept(any());
		assertEquals(values, dogConsumer.getOrElse(key).getValues());
		verify(scheduler, never()).justExecuted();
	}

	@Test
	public void runReaperNoElements() throws Exception {

		Rule rule = mock(Rule.class);

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque<>(), scheduler, Arrays.asList(rule), null, Clock.systemUTC(), ruleExecutorTimeout, callback);
		dogConsumer.checkTimeouts();

		verify(callback, never()).accept(any());
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, never()).execute(any(Element.class), any());
		verify(scheduler, times(1)).justExecuted();
	}

	@Test
	public void runReaper2Rules() {

		Rule rule1 = mock(Rule.class);
		Rule rule2 = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule1, rule2);
		when(ruleExecutorTimeout.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values));

		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.ofEpochMilli(15));

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque(), scheduler, rules, Duration.ofMillis(10), clock, ruleExecutorTimeout, callback);
		dogConsumer.put(element);
		dogConsumer.checkTimeouts();

		verify(callback, times(1)).accept(values);
		verify(scheduler, times(1)).justExecuted();
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, times(1)).execute(any(Element.class), any());
		assertTrue(dogConsumer.getOrElse(key).getValues().isEmpty());
	}

	@Test
	public void runReaperKeepResultAfterGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutorTimeout.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values, element));
		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.ofEpochMilli(15));

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque(), scheduler, rules, Duration.ofMillis(10), clock, ruleExecutorTimeout, callback);
		dogConsumer.put(element);
		dogConsumer.checkTimeouts();

		verify(callback, times(1)).accept(values);
		verify(scheduler, times(1)).justExecuted();
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, times(1)).execute(any(Element.class), any());
		assertEquals(values, dogConsumer.getOrElse(key).getValues());
	}

	@Test
	public void runReaperKeepResultAfterNoGroup() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutorTimeout.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canNotGroup(element));
		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.ofEpochMilli(15));

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque(), scheduler, Arrays.asList(rule), Duration.ofMillis(10), clock, ruleExecutorTimeout, callback);
		dogConsumer.put(element);
		dogConsumer.checkTimeouts();

		verify(callback, never()).accept(values);
		verify(scheduler, times(1)).justExecuted();
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, times(1)).execute(any(Element.class), any());
		assertEquals(values, dogConsumer.getOrElse(key).getValues());
	}

	@Test
	public void runReaperNoTimeout() throws Exception {

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutorTimeout.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canNotGroup(element));
		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.ofEpochMilli(5));

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque(), scheduler, rules, Duration.ofMillis(10), clock, ruleExecutorTimeout, callback);
		dogConsumer.put(element);
		dogConsumer.checkTimeouts();

		verify(callback, never()).accept(values);
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, never()).execute(any(Element.class), any());
		verify(scheduler, times(1)).justExecuted();
		assertEquals(values, dogConsumer.getOrElse(key).getValues());
	}

	@Test
	public void runReaperStopBefore() throws Exception {

		String key2 = "key2";
		Element element2 = new Element(key2, Arrays.asList(4, 5, 6), 10l);

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutorTimeout.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values));
		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.ofEpochMilli(15));

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque(), scheduler, rules, Duration.ofMillis(10), clock, ruleExecutorTimeout, callback);
		dogConsumer.put(element);
		dogConsumer.put(element2);
		dogConsumer.checkTimeouts();

		verify(callback, times(1)).accept(any());
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, times(1)).execute(any(Element.class), any());
		verify(scheduler, times(1)).justExecuted();
		assertTrue(dogConsumer.getOrElse(key).getValues().isEmpty());
		assertFalse(dogConsumer.getOrElse(key2).getValues().isEmpty());
	}

	@Test
	public void runReaperStopBefore2() throws Exception {

		String key1 = "key1";
		Element element1 = new Element(key1, Arrays.asList(1, 2, 3), 10l);
		String key2 = "key2";
		Element element2 = new Element(key2, Arrays.asList(4, 5, 6), 15l);

		Rule rule = mock(Rule.class);
		List<Rule> rules = Arrays.asList(rule);
		when(ruleExecutorTimeout.execute(any(Element.class), eq(rules))).thenReturn(RuleResult.canGroup(values));
		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.ofEpochMilli(15));

		DogConsumer dogConsumer = new DogConsumer(null, ruleExecutor, new LinkedBlockingDeque(), scheduler, rules, Duration.ofMillis(10), clock, ruleExecutorTimeout, callback);
		dogConsumer.put(element1);
		dogConsumer.put(element2);
		dogConsumer.checkTimeouts();

		verify(callback, never()).accept(any());
		verify(rule, never()).canGroup(any());
		verify(ruleExecutor, never()).execute(any(Element.class), any());
		verify(ruleExecutorTimeout, never()).execute(any(Element.class), any());
		verify(scheduler, times(1)).justExecuted();
		assertFalse(dogConsumer.getOrElse(key1).getValues().isEmpty());
		assertFalse(dogConsumer.getOrElse(key2).getValues().isEmpty());
	}
}
