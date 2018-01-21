package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.ResultsPool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.security.Key;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShepherdTest {

	@Test(expected = IllegalArgumentException.class)
	public void threadsNegative() throws Exception {
		Shepherd.create().basic(new DummyKeyExtractor(), Optional.empty(), (a) -> {
		}).threads(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void threads0() throws Exception {
		Shepherd.create().basic(new DummyKeyExtractor(), Optional.empty(), (a) -> {
		}).threads(0);
	}

	@Test(expected = NullPointerException.class)
	public void nullKeyExtractor() throws Exception {
		Shepherd.create().basic(null, Optional.empty(), (a) -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void nullCallback() throws Exception {
		Shepherd.create().basic(new DummyKeyExtractor(), Optional.empty(), null);
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

	@Test(expected = IllegalArgumentException.class)
	public void syncAndDog() throws Exception {
		Shepherd.create().sync().withDog(Duration.ofMillis(1), new ArrayList<>());
	}

	@Test
	public void addNull() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenReturn(null);

		Shepherd shepherd = Shepherd.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).build();
		assertFalse(shepherd.add(o));
	}

	@Test
	public void addException() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenThrow(new Exception());

		Shepherd shepherd = Shepherd.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).build();
		assertFalse(shepherd.add(o));
	}

	@Test
	public void add() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenReturn(o);

		Shepherd shepherd = Shepherd.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).build();
		assertTrue(shepherd.add(o));
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
