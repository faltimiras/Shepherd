package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.ListCollector;
import org.junit.Test;

import java.util.Optional;
import java.util.function.Function;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShepherdSyncTest {

	@Test
	public void addNull() throws Exception{
		Object o = new Object();
		Function keyExtractor = mock(Function.class);
		when(keyExtractor.apply(o)).thenReturn(null);

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						keyExtractor,
						Optional.empty(),
						new ListCollector())
				.buildSync();

		assertFalse(shepherd.add(o));
	}

	@Test
	public void add() throws Exception{
		Object o = new Object();
		Function keyExtractor = mock(Function.class);
		when(keyExtractor.apply(o)).thenReturn(o);

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						keyExtractor,
						Optional.empty(),
						new ListCollector())
				.buildSync();

		assertTrue(shepherd.add(o));
	}
}