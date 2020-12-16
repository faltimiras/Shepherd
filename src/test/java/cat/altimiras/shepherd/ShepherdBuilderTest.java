package cat.altimiras.shepherd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import cat.altimiras.shepherd.rules.RuleWindow;
import cat.altimiras.shepherd.rules.window.GroupExpiredSlidingRule;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class ShepherdBuilderTest {

	private final Consumer callback = mock(Consumer.class);

	private final List rules = new ArrayList();

	private final RuleWindow ruleWindow = mock(RuleWindow.class);

	private final Function keyExtractor = mock(Function.class);

	@Test
	public void threadsNegative() {
		assertThrows(IllegalArgumentException.class, () ->
				ShepherdBuilder.create()
						.basic(
								(a) -> {
								})
						.threads(-1));
	}

	@Test
	public void threads0() {
		assertThrows(IllegalArgumentException.class, () ->
				ShepherdBuilder.create()
						.basic(
								(a) -> {
								})
						.threads(0));
	}

	@Test
	public void nullCallback() {
		assertThrows(NullPointerException.class, () ->
				ShepherdBuilder.create()
						.basic(
								null));
	}

	@Test
	public void nullDuration() {
		assertThrows(NullPointerException.class, () ->
				ShepherdBuilder.create()
						.withWindow(
								null,
								new GroupExpiredSlidingRule(null, false, Clock.systemUTC())));
	}

	@Test
	public void nullRulesTimeout() {
		assertThrows(NullPointerException.class, () ->
				ShepherdBuilder.create()
						.withWindow(
								Duration.ofMillis(1),
								null));
	}

	@Test
	public void withWindowLater() throws Exception {
		ShepherdBuilder builder = ShepherdBuilder.create()
				.basic(keyExtractor, callback, rules);
		builder.withWindow(Duration.ofMillis(123), ruleWindow);

		ShepherdASync shepherd = builder.build();

		assertNotNull(shepherd);
		assertEquals(ruleWindow, shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123), shepherd.getWindow().getPrecision());
	}

	@Test
	public void withWindowLaterSync() throws Exception {
		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(callback, rules)
				.withWindow(Duration.ofMillis(123), ruleWindow)
				.buildSync();

		assertNotNull(shepherd);
		assertEquals(ruleWindow, shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123), shepherd.getWindow().getPrecision());
	}

	@Test
	public void withWindow() throws Exception {
		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(callback, rules)
				.withWindow(Duration.ofMillis(123), ruleWindow)
				.build();

		assertNotNull(shepherd);
		assertEquals(ruleWindow, shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123), shepherd.getWindow().getPrecision());
	}

	@Test
	public void withWindowSync() throws Exception {
		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(callback, rules)
				.withWindow(Duration.ofMillis(123), ruleWindow)
				.buildSync();

		assertNotNull(shepherd);
		assertEquals(ruleWindow, shepherd.getWindow().getRule());
		assertEquals(Duration.ofMillis(123), shepherd.getWindow().getPrecision());
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