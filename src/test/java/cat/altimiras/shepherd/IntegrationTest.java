package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.FileCollector;
import cat.altimiras.shepherd.callback.ListCollector;
import cat.altimiras.shepherd.rules.AccumulateNRule;
import cat.altimiras.shepherd.rules.AccumulateRule;
import cat.altimiras.shepherd.rules.DiscardAllExpiredRule;
import cat.altimiras.shepherd.rules.GroupAllExpiredRule;
import cat.altimiras.shepherd.rules.NoDuplicatesRule;
import cat.altimiras.shepherd.rules.keyextractors.SameKeyExtractor;
import cat.altimiras.shepherd.rules.keyextractors.SimpleKeyExtractor;
import cat.altimiras.shepherd.storage.file.FileValuesStorage;
import cat.altimiras.shepherd.storage.redis.RedisValuesStorage;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@Ignore
@SuppressWarnings({"unchecked", "rawtypes"})
public class IntegrationTest {

	//THIS DON'T PRETEND TO BE A UNIT TEST.

	@Test
	public void noDuplicates() throws Exception {

		ListCollector<Integer> listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						Optional.of(Collections.singletonList(new NoDuplicatesRule())),
						listCollector)
				.threads(1)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		assertEquals(2, listCollector.size());
		assertEquals(1, listCollector.get(1).get(0).get(0).intValue());
		assertEquals(2, listCollector.get(1).get(0).get(0).intValue());

	}

	@Test
	public void accumulate() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						Optional.of(Collections.singletonList(new AccumulateNRule(2))),
						listCollector)
				.threads(1)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(3);
		shepherd.add(3);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		assertEquals(3, listCollector.size());
		List<List> result = listCollector.get();
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
	public void accumulateMultiPartition() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						Optional.of(Collections.singletonList(new AccumulateNRule(2))),
						listCollector)
				.threads(2)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(3);
		shepherd.add(3);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		assertEquals(3, listCollector.size());
		List<List> result = listCollector.get();
		//as it is multithread order is not predectible
		assertEquals(2, result.get(0).size());
		assertEquals(result.get(0).get(0), result.get(0).get(1));
		assertEquals(2, result.get(1).size());
		assertEquals(result.get(1).get(0), result.get(1).get(1));
		assertEquals(2, result.get(2).size());
		assertEquals(result.get(2).get(0), result.get(2).get(1));
	}

	@Test
	public void accumulate2() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						Optional.of(Collections.singletonList(new AccumulateNRule(2))),
						listCollector)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(3);
		shepherd.add(3);
		shepherd.add(3);
		shepherd.add(2);
		shepherd.add(2);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		assertEquals(4, listCollector.size());
		List<List> result = listCollector.get();
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
	public void noRepeatsInWindows() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						Optional.of(Collections.singletonList(new NoDuplicatesRule())),
						listCollector)
				.threads(1)
				.withDog(
						Duration.ofMillis(10),
						Collections.singletonList(new DiscardAllExpiredRule(Duration.ofMillis(50), false)))
				.build();

		shepherd.add(1);
		shepherd.add(1);
		Thread.sleep(100);
		shepherd.add(1);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		assertEquals(2, listCollector.size());
		List<List<Integer>> result = listCollector.get();
		assertEquals(1, result.get(0).get(0).intValue());
		assertEquals(1, result.get(1).get(0).intValue());
	}

	@Test
	public void accumulateWindows() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						Optional.empty(),
						listCollector)
				.threads(1)
				.withDog(
						Duration.ofMillis(10),
						Collections.singletonList(new GroupAllExpiredRule(Duration.ofMillis(100), false)))
				.build();

		shepherd.add("lolo");
		shepherd.add("lala");
		shepherd.add("lele");
		Thread.sleep(50);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		assertEquals(3, listCollector.size());
		List<List<String>> result = listCollector.get();
		assertEquals("lolo", result.get(0).get(0));
		assertEquals("lala", result.get(1).get(0));
		assertEquals("lele", result.get(2).get(0));
	}

	@Test
	public void accumulateContentWindowsInFile() throws Exception {

		FileCollector fileCollector = new FileCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SameKeyExtractor(),
						Optional.of(Collections.singletonList(new AccumulateRule())),
						fileCollector)
				.threads(1)
				.withValuesStorageProvider(FileValuesStorage::new)
				.withDog(
						Duration.ofMillis(10),
						Collections.singletonList(new GroupAllExpiredRule(Duration.ofMillis(1000), false)))
				.build();

		shepherd.add("lolo");
		shepherd.add("lala");
		shepherd.add("lele");
		Thread.sleep(1000);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		List<Path> result = fileCollector.get();
		assertEquals(1, result.size());

		assertEquals("lololalalele", new String(Files.readAllBytes(result.get(0))));
	}

	@Test
	public void accumulateContentWindowsInRedis() throws Exception {

		ListCollector binaryCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SameKeyExtractor(),
						Optional.of(Collections.singletonList(new AccumulateRule())),
						binaryCollector)
				.threads(1)
				.withValuesStorageProvider(RedisValuesStorage::new)
				.withDog(
						Duration.ofMillis(10),
						Collections.singletonList(new GroupAllExpiredRule(Duration.ofMillis(1000), false)))
				.build();

		shepherd.add("lolo");
		shepherd.add("lala");
		shepherd.add("lele");
		Thread.sleep(1000);

		await().atMost(1, SECONDS).until(shepherd::areQueuesEmpty);

		List<String> result = binaryCollector.get();
		assertEquals(1, result.size());

		assertEquals("lololalalele", result.get(0));
	}
}