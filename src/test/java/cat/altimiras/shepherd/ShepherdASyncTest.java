package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.ListCollector;
import org.junit.Test;

import java.util.function.Function;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShepherdASyncTest {

	@Test
	public void addNull() throws Exception {
		Object o = new Object();
		Function keyExtractor = mock(Function.class);
		when(keyExtractor.apply(o)).thenReturn(null);

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(keyExtractor,
						new ListCollector())
				.build();

		assertFalse(shepherd.add(o));
	}

	@Test
	public void add() throws Exception {
		Object o = new Object();
		Function keyExtractor = mock(Function.class);
		when(keyExtractor.apply(o)).thenReturn(o);

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						keyExtractor,
						new ListCollector())
				.build();

		assertTrue(shepherd.add(o));
	}
}