package cat.altimiras.shepherd;

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

public class ShepherdBuilderTest {

	@Test(expected = IllegalArgumentException.class)
	public void threadsNegative() throws Exception {
		ShepherdBuilder.create()
				.basic(
						new DummyKeyExtractor(),
						Optional.empty(), (a) -> {
						})
				.threads(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void threads0() throws Exception {
		ShepherdBuilder.create()
				.basic(
						new DummyKeyExtractor(),
						Optional.empty(), (a) -> {
						})
				.threads(0);
	}

	@Test(expected = NullPointerException.class)
	public void nullCallback() throws Exception {
		ShepherdBuilder.create()
				.basic(
						new DummyKeyExtractor(),
						Optional.empty(),
						null);
	}

	@Test(expected = NullPointerException.class)
	public void nullDuration() throws Exception {
		ShepherdBuilder.create()
				.withDog(
						null,
						Arrays.asList(new DummyRule()));
	}

	@Test(expected = NullPointerException.class)
	public void nullRulesTimeout() throws Exception {
		ShepherdBuilder.create()
				.withDog(
						Duration.ofMillis(1),
						null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void emptyRulesTimeout() throws Exception {
		ShepherdBuilder.create()
				.withDog(
						Duration.ofMillis(1),
						new ArrayList<>());
	}

	private class DummyKeyExtractor implements Function {
		@Override
		public Object apply(Object o) {
			return null;
		}
	}

	private class DummyRule implements Rule {
		@Override
		public RuleResult canGroup(Metadata metadata, Object value, LazyValue lazyValue) {
			return null;
		}
	}


}
