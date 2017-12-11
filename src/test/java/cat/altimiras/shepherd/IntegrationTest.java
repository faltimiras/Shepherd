package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.ResultsPool;
import cat.altimiras.shepherd.rules.AccumulateNRule;
import cat.altimiras.shepherd.rules.keyextractors.NoDuplicatesKeyExtractor;
import cat.altimiras.shepherd.rules.NoDuplicatesRule;
import cat.altimiras.shepherd.rules.keyextractors.SimpleKeyExtractor;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Ignore
public class IntegrationTest {

	//THIS IS NOT UNIT TEST. KIND OF INTEGRATION TEST!

	@Test
	public void noDuplicates() throws Exception{

		ResultsPool<Integer> resultsPool = new ResultsPool();

		Shepherd shepherd = Shepherd.create().basic(1, new NoDuplicatesKeyExtractor(), Optional.of(Arrays.asList(new NoDuplicatesRule())), resultsPool).build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		Thread.sleep(100);

		assertEquals(3,resultsPool.size());
		assertEquals(1,  resultsPool.get(1).get(0).get(0).intValue());
		assertEquals(2,  resultsPool.get(1).get(0).get(0).intValue());
		assertEquals(1,  resultsPool.get(1).get(0).get(0).intValue());

	}

	@Test
	public void accumulate() throws Exception{

		ResultsPool resultsPool = new ResultsPool();

		Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.of(Arrays.asList(new AccumulateNRule(2))), resultsPool).build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(3);
		shepherd.add(3);
		Thread.sleep(100);

		assertEquals(3,resultsPool.size());
		List<List> result = resultsPool.get();
		assertEquals(2, result.get(0).size());
		assertEquals(2, result.get(0).get(0));
		assertEquals(2, result.get(0).get(1));
		assertEquals(2, result.get(1).size());
		assertEquals(1, result.get(1).get(0));
		assertEquals(1, result.get(1).get(1));
		assertEquals(2, result.get(2).size());
		assertEquals(3, result.get(2).get(0));
		assertEquals(3, result.get(2).get(1));

	}

	@Test
	public void accumulate2() throws Exception{

		ResultsPool resultsPool = new ResultsPool();

		Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.of(Arrays.asList(new AccumulateNRule(2))), resultsPool).build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(3);
		shepherd.add(3);
		shepherd.add(3);
		shepherd.add(2);
		shepherd.add(2);
		Thread.sleep(100);

		assertEquals(4,resultsPool.size());
		List<List> result = resultsPool.get();
		assertEquals(2, result.get(0).size());
		assertEquals(2, result.get(0).get(0));
		assertEquals(2, result.get(0).get(1));
		assertEquals(2, result.get(1).size());
		assertEquals(1, result.get(1).get(0));
		assertEquals(1, result.get(1).get(1));
		assertEquals(2, result.get(2).size());
		assertEquals(3, result.get(2).get(0));
		assertEquals(3, result.get(2).get(1));
		assertEquals(2, result.get(3).size());
		assertEquals(2, result.get(3).get(0));
		assertEquals(2, result.get(3).get(1));
	}

	@Test
	public void accumulateTimeout() throws Exception{

		ResultsPool resultsPool = new ResultsPool();

		Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.empty(), resultsPool).withDog(Duration.ofMillis(50), Arrays.asList(new AccumulateNRule(2))).build();

		shepherd.add(1);
		shepherd.add(1);
		shepherd.add(1);
		Thread.sleep(200);

		List<Integer> result = (List<Integer>) resultsPool.get().get(0);
		assertEquals(3, result.size());
		assertEquals(1, result.get(0).intValue());
		assertEquals(1, result.get(1).intValue());
		assertEquals(1, result.get(2).intValue());
	}

	@Test
	public void accumulateTimeout2() throws Exception{

		ResultsPool resultsPool = new ResultsPool();

		Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.empty(), resultsPool).withDog(Duration.ofMillis(50), Arrays.asList(new AccumulateNRule(2))).build();

		shepherd.add(1);
		shepherd.add(1);
		shepherd.add(1);
		Thread.sleep(200);

		List<Integer> result = (List<Integer>) resultsPool.get().get(0);
		assertEquals(3, result.size());
		assertEquals(1, result.get(0).intValue());
		assertEquals(1, result.get(1).intValue());
		assertEquals(1, result.get(2).intValue());

		shepherd.add(1);
		Thread.sleep(200);

		assertEquals(0, resultsPool.get().size());

		shepherd.add(2);
		Thread.sleep(200);

		assertEquals(0, resultsPool.get().size());

		shepherd.add(2);
		shepherd.add(1);
		Thread.sleep(200);

		List<List<Integer>> result2 = resultsPool.get();
		assertEquals(2, result2.size());
		assertEquals(2, result2.get(0).size());
		assertEquals(1, result2.get(0).get(0).intValue());
		assertEquals(1, result2.get(0).get(1).intValue());
		assertEquals(2, result2.get(1).size());
		assertEquals(2, result2.get(1).get(0).intValue());
		assertEquals(2, result2.get(1).get(1).intValue());
	}

}
