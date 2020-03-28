package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.window.GroupAllExpiredRule;
import org.junit.Test;

import java.time.Duration;

public class ShepherdBuilderTest {

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
						new GroupAllExpiredRule(null, false));
	}

	@Test(expected = NullPointerException.class)
	public void nullRulesTimeout() throws Exception {
		ShepherdBuilder.create()
				.withWindow(
						Duration.ofMillis(1),
						null);
	}
}