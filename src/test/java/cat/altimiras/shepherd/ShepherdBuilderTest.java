package cat.altimiras.shepherd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import cat.altimiras.shepherd.rules.RuleWindow;
import cat.altimiras.shepherd.rules.window.GroupExpiredSlidingRule;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import org.mockito.Mockito;

public class ShepherdBuilderTest {

	private Consumer callback = mock(Consumer.class);
	private List rules = new ArrayList();
	private RuleWindow ruleWindow = mock(RuleWindow.class);
	private Function keyExtractor = mock(Function.class);

	@Test(expected = IllegalArgumentException.class)
	public void threadsNegative() throws Exception {
		ShepherdBuilder.create()
				.basic(
						(a) -> {
						})
				.threads(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void threads0() throws Exception {
		ShepherdBuilder.create()
				.basic(
						(a) -> {
						})
				.threads(0);
	}

	@Test(expected = NullPointerException.class)
	public void nullCallback() throws Exception {
		ShepherdBuilder.create()
				.basic(
						null);
	}

	@Test(expected = NullPointerException.class)
	public void nullDuration() throws Exception {
		ShepherdBuilder.create()
				.withWindow(
						null,
						new GroupExpiredSlidingRule(null, false));
	}

	@Test(expected = NullPointerException.class)
	public void nullRulesTimeout() throws Exception {
		ShepherdBuilder.create()
				.withWindow(
						Duration.ofMillis(1),
						null);
	}

	@Test
	public void withWindowLater() throws Exception {
		ShepherdBuilder builder = ShepherdBuilder.create()
				.basic(keyExtractor, callback, rules);
		builder.withWindow(Duration.ofMillis(123),ruleWindow);

		ShepherdASync shepherd = builder.build();

		assertNotNull(shepherd);
		assertEquals(ruleWindow,shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123),shepherd.getWindow().getPrecision());
	}

	@Test
	public void withWindowLaterSync() throws Exception {
		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(callback, rules)
				.withWindow(Duration.ofMillis(123),ruleWindow)
				.buildSync();

		assertNotNull(shepherd);
		assertEquals(ruleWindow,shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123),shepherd.getWindow().getPrecision());
	}

	@Test
	public void withWindow() throws Exception {
		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(callback, rules)
				.withWindow(Duration.ofMillis(123),ruleWindow)
				.build();

		assertNotNull(shepherd);
		assertEquals(ruleWindow,shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123),shepherd.getWindow().getPrecision());
	}

	@Test
	public void withWindowSync() throws Exception {
		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(callback, rules)
				.withWindow(Duration.ofMillis(123),ruleWindow)
				.buildSync();

		assertNotNull(shepherd);
		assertEquals(ruleWindow,shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123),shepherd.getWindow().getPrecision());
	}

	@Test
	public void noWindowLater() throws Exception {
		ShepherdBuilder builder = ShepherdBuilder.create()
				.basic(callback, rules);

		ShepherdASync shepherd = builder.build();

		assertNotNull(shepherd);
		assertNull(shepherd.getWindow());
	}

	@Test
	public void noWindowLaterSync() throws Exception {
		ShepherdBuilder builder = ShepherdBuilder.create()
				.basic(callback, rules);

		ShepherdSync shepherd = builder.buildSync();

		assertNotNull(shepherd);
		assertNull(shepherd.getWindow());
	}



}