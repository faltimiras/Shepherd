package cat.altimiras.shepherd;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cat.altimiras.shepherd.callback.ListCollector;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class ShepherdSyncTest {

	@Test
	public void addNull() throws Exception {
		Object o = new Object();
		Function keyExtractor = mock(Function.class);
		when(keyExtractor.apply(o)).thenReturn(null);

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						keyExtractor,
						new ListCollector())
				.buildSync();

		assertFalse(shepherd.add(o));
	}

	@Test
	public void add() throws Exception {
		Object o = new Object();
		Function keyExtractor = mock(Function.class);
		when(keyExtractor.apply(o)).thenReturn(o);

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						keyExtractor,
						new ListCollector())
				.buildSync();

		assertTrue(shepherd.add(o));
	}
}