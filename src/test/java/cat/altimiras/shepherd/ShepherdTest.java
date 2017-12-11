package cat.altimiras.shepherd;

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class ShepherdTest {

	@Test(expected = IllegalArgumentException.class)
	public void threadsNegative() throws Exception {
		Shepherd.create().basic(-1, new DummyKeyExtractor(), Optional.empty(), (a) -> {
		});
	}

	@Test(expected = IllegalArgumentException.class)
	public void threads0() throws Exception {
		Shepherd.create().basic(0, new DummyKeyExtractor(), Optional.empty(), (a) -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void nullKeyExtractor() throws Exception {
		Shepherd.create().basic(1, null, Optional.empty(), (a) -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void nullCallback() throws Exception {
		Shepherd.create().basic(1, new DummyKeyExtractor(), Optional.empty(), null);
	}

	@Test(expected = NullPointerException.class)
	public void nullDuration() throws Exception {
		Shepherd.create().withDog(null, Arrays.asList(new DummyRule()));
	}

	@Test(expected = NullPointerException.class)
	public void nullRulesTimeout() throws Exception {
		Shepherd.create().withDog(Duration.ofMillis(1), null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void emptyRulesTimeout() throws Exception {
		Shepherd.create().withDog(Duration.ofMillis(1), new ArrayList<>());
	}



	private class DummyKeyExtractor implements KeyExtractor {
		@Override
		public Object key(Object o) {
			return null;
		}
	}

	private class DummyRule implements Rule {
		@Override
		public RuleResult canGroup(Element element) {
			return null;
		}
	}


}
