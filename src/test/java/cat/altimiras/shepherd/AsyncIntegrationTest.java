package cat.altimiras.shepherd;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import cat.altimiras.shepherd.callback.FileCollector;
import cat.altimiras.shepherd.callback.ListCollector;
import cat.altimiras.shepherd.rules.keyextractors.FixedKeyExtractor;
import cat.altimiras.shepherd.rules.keyextractors.SimpleKeyExtractor;
import cat.altimiras.shepherd.rules.streaming.AccumulateNRule;
import cat.altimiras.shepherd.rules.streaming.AccumulateRule;
import cat.altimiras.shepherd.rules.streaming.NoDuplicatesRule;
import cat.altimiras.shepherd.rules.streaming.SumRule;
import cat.altimiras.shepherd.rules.window.AvgTumblingRule;
import cat.altimiras.shepherd.rules.window.DiscardExpiredSlidingRule;
import cat.altimiras.shepherd.rules.window.GroupExpiredSlidingRule;
import cat.altimiras.shepherd.rules.window.GroupExpiredTumblingWindowRule;
import cat.altimiras.shepherd.storage.file.FileValuesStorage;
import cat.altimiras.shepherd.storage.memory.InMemoryValuesStorage;
import cat.altimiras.shepherd.storage.redis.RedisValuesStorage;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

@SuppressWarnings({"unchecked", "rawtypes"})
public class AsyncIntegrationTest {

	//This test contains sleeps to replicate real life (of course without exit) but it is useful to double check and understand.
	//Due to async & multi thread approach there is no other way to "test" it. Suggestion on how to do it better, of course are always welcome.

	//@Test
	public void noDuplicates() throws Exception {

		ListCollector<Integer> listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector,
						new NoDuplicatesRule())
				.threads(1)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);

		await().atMost(1, SECONDS)
				.until(shepherd::areQueuesEmpty);

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
						listCollector,
						new AccumulateNRule(2))
				.threads(1)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(3);
		shepherd.add(3);

		await().atMost(1, SECONDS)
				.until(shepherd::areQueuesEmpty);

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
						listCollector,
						new AccumulateNRule(2))
				.threads(2)
				.build();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(3);
		shepherd.add(3);

		await().atMost(1, SECONDS)
				.until(shepherd::areQueuesEmpty);

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
						listCollector,
						new AccumulateNRule(2))
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

		await().atMost(1, SECONDS)
				.until(shepherd::areQueuesEmpty);

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
	public void noRepeatsInSlidingWindows() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector,
						new NoDuplicatesRule())
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new DiscardExpiredSlidingRule(Duration.ofMillis(50), false, Clock.systemUTC()))
				.build();

		shepherd.add(1);
		shepherd.add(1);
		Thread.sleep(100);
		shepherd.add(1);

		await()
				.atMost(65, MILLISECONDS)
				.pollInterval(5, MILLISECONDS)
				.until(() -> listCollector.size() == 2);

		assertEquals(2, listCollector.size());
		List<List<Integer>> result = listCollector.get();
		assertEquals(1, result.get(0).get(0).intValue());
		assertEquals(1, result.get(1).get(0).intValue());
	}

	@Test
	public void accumulateSlidingWindows() throws Exception {

		Clock clock = Clock.systemUTC();

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(100), false, clock))
				.build();

		shepherd.add("lele", clock.millis());
		shepherd.add("lolo", clock.millis());
		shepherd.add("lala", clock.millis());

		await().atLeast(100, MILLISECONDS)
				.atMost(101, SECONDS)
				.until(() -> listCollector.size() == 3);

		assertEquals(3, listCollector.size());
		List<List<String>> result = listCollector.get();
		result.sort(Comparator.comparing(strings -> strings.get(0))); //order is not deterministic
		assertEquals("lala", result.get(0).get(0));
		assertEquals("lele", result.get(1).get(0));
		assertEquals("lolo", result.get(2).get(0));
	}

	@Test
	public void accumulateSlidingWindowsInThePast() throws Exception {
		Instant oneMomentInThePast = LocalDateTime.of(2015, Month.FEBRUARY, 20, 06, 30, 55).atZone(ZoneId.systemDefault()).toInstant();

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(100), false, Clock.systemUTC()))
				.build();

		shepherd.add("lele", oneMomentInThePast.toEpochMilli());
		Thread.sleep(30);
		shepherd.add("lolo", oneMomentInThePast.plusMillis(10).toEpochMilli());
		assertEquals(0, listCollector.size()); //window is not closed yet, any element should be collected

		await().atLeast(70, MILLISECONDS)
				.atMost(115, MILLISECONDS)
				.pollInterval(5, MILLISECONDS)
				.until(() -> listCollector.size() == 2); //110 window + precision

		assertEquals(2, listCollector.size());
		List<List<String>> result = listCollector.get();
		result.sort(Comparator.comparing(strings -> strings.get(0))); //order is not deterministic
		assertEquals("lele", result.get(0).get(0));
		assertEquals("lolo", result.get(1).get(0));
	}

	@Test
	public void accumulateContentSlidingWindowsInFile() throws Exception {

		FileCollector fileCollector = new FileCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						fileCollector,
						new AccumulateRule())
				.threads(1)
				.withValuesStorageProvider(FileValuesStorage::new)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(1000), false, Clock.systemUTC()))
				.build();

		shepherd.add("lolo");
		shepherd.add("lala");
		shepherd.add("lele");

		await().atMost(1021, MILLISECONDS)
				.pollInterval(5, MILLISECONDS)
				.until(() -> fileCollector.size() == 1);

		List<Path> result = fileCollector.get();
		assertEquals(1, result.size());

		assertEquals("lololalalele", new String(Files.readAllBytes(result.get(0))));

		//release resources
		deleteDir();
		shepherd.stop(true);
	}

	@Test
	public void accumulateContentSlidingWindowsInRedis() throws Exception {
		String keyUsed = "shepherd-redis-key-" + new Random().nextLong();

		//clean redis
		cleanRedis(keyUsed);

		ListCollector binaryCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(keyUsed),
						binaryCollector,
						new AccumulateRule())
				.threads(1)
				.withValuesStorageProvider(RedisValuesStorage::new)
				.withWindow(
						Duration.ofMillis(100),
						new GroupExpiredSlidingRule(Duration.ofMillis(1000), false, Clock.systemUTC()))
				.build();

		shepherd.add("lolo");
		shepherd.add("lala");
		shepherd.add("lele");

		await().atLeast(990, MILLISECONDS)
				.and()
				.atMost(1100, MILLISECONDS)
				.pollInterval(10, MILLISECONDS)
				.until(() -> binaryCollector.size() == 1);

		List<String> result = binaryCollector.get();
		assertEquals(1, result.size());

		assertEquals("lololalalele", result.get(0));

		//clean redis
		cleanRedis(keyUsed);
	}

	@Test
	public void accumulateFixedWindowInMemory() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredTumblingWindowRule(Duration.ofMillis(100), Clock.systemUTC()))
				.build();

		shepherd.add("lolo", 0);
		shepherd.add("lala", 10);
		shepherd.add("lele", 110);

		await().atMost(110, SECONDS)
				.pollInterval(5, MILLISECONDS)
				.until(() -> listCollector.size() == 2);

		List<List<String>> result = listCollector.get();
		result.sort(Comparator.comparing(List::size));
		assertEquals(2, result.size());
		assertEquals(1, result.get(0).size());
		assertEquals("lele", result.get(0).get(0));
		assertEquals(2, result.get(1).size());
		assertEquals("lolo", result.get(1).get(0));
		assertEquals("lala", result.get(1).get(1));
	}

	@Test
	public void sumValuesInWindow() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						listCollector,
						new SumRule())
				.threads(1)
				.withValuesStorageProvider(InMemoryValuesStorage::new)
				.withWindow(
						Duration.ofMillis(20),
						new GroupExpiredSlidingRule(Duration.ofMillis(500), false, Clock.systemUTC()))
				.build();

		shepherd.add(Long.valueOf(11), 0);
		shepherd.add(Long.valueOf(22), 10);
		shepherd.add(Long.valueOf(33), 110);

		await().atMost(520, SECONDS)
				.pollInterval(5, MILLISECONDS)
				.until(() -> listCollector.size() == 1);

		List<Number> result = listCollector.get();
		assertEquals(1, result.size());
		assertEquals(66L, result.get(0).longValue());
	}

	@Test
	public void avgInWindow() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdASync shepherd = ShepherdBuilder.create()
				.basic(
						listCollector,
						new SumRule())
				.threads(1)
				.withValuesStorageProvider(InMemoryValuesStorage::new)
				.withWindow(
						Duration.ofMillis(50),
						new AvgTumblingRule(Duration.ofMillis(200), Duration.ofMillis(100), Clock.systemUTC()))
				.build();

		shepherd.add("k1", 10L, 0);
		shepherd.add("k2", 11L, 0);
		shepherd.add("k1", 20L, 10);
		shepherd.add("k1", 30L, 110);

		await().atLeast(90, MILLISECONDS) //only waits the delay
				.atMost(260, MILLISECONDS)
				.pollInterval(5, MILLISECONDS)
				.until(() -> listCollector.size() == 2);

		List<Number> result = listCollector.get();
		assertEquals(2, result.size());
		assertEquals(20.0, result.get(0).longValue(), 0);
		assertEquals(11.0, result.get(1).longValue(), 0);
	}

	private void deleteDir() {
		deleteDir(Paths.get(System.getProperty("java.io.tmpdir"), "shepherd"));
	}

	private void deleteDir(Path path) {
		try {
			Files.walk(path)
					.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.forEach(File::delete);
		} catch (Exception e) {
			//nothing to do
		}
	}

	private void cleanRedis(String key) {
		Jedis jedis = new Jedis();
		jedis.del(key);
	}
}