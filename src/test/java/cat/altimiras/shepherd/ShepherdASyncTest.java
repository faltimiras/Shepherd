package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.ResultsPool;
import org.junit.Test;

import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShepherdASyncTest {

	@Test
	public void addNull() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenReturn(null);

		ShepherdASync shepherd = ShepherdBuilder.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).build();
		assertFalse(shepherd.add(o));
	}

	@Test
	public void addException() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenThrow(new Exception());

		ShepherdASync shepherd = ShepherdBuilder.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).build();
		assertFalse(shepherd.add(o));
	}

	@Test
	public void add() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenReturn(o);

		ShepherdASync shepherd = ShepherdBuilder.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).build();
		assertTrue(shepherd.add(o));
	}
}
