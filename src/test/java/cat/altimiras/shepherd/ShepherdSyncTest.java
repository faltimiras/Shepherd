package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.ResultsPool;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

public class ShepherdSyncTest {

	@Test
	public void addNull() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenReturn(null);

		ShepherdSync shepherd = ShepherdBuilder.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).buildSync();
		assertFalse(shepherd.add(o));
	}

	@Test
	public void addException() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenThrow(new Exception());

		ShepherdSync shepherd = ShepherdBuilder.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).buildSync();
		assertFalse(shepherd.add(o));
	}

	@Test
	public void add() throws Exception{
		Object o = new Object();
		KeyExtractor keyExtractor = mock(KeyExtractor.class);
		when(keyExtractor.key(o)).thenReturn(o);

		ShepherdSync shepherd = ShepherdBuilder.create().basic(keyExtractor, Optional.empty(), new ResultsPool()).buildSync();
		assertTrue(shepherd.add(o));
	}
}