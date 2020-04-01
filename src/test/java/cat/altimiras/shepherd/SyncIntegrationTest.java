package cat.altimiras.shepherd;

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
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SyncIntegrationTest {

	@Test
	public void noDuplicates() throws Exception {

		ListCollector<Integer> listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector,
						new NoDuplicatesRule())
				.threads(1)
				.buildSync();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);

		assertEquals(2, listCollector.size());
		assertEquals(1, listCollector.get(1).get(0).get(0).intValue());
		assertEquals(2, listCollector.get(1).get(0).get(0).intValue());
	}

	@Test
	public void accumulate() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector,
						new AccumulateNRule(2))
				.threads(1)
				.buildSync();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(3);
		shepherd.add(3);

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
	public void accumulate2() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector,
						new AccumulateNRule(2))
				.buildSync();

		shepherd.add(1);
		shepherd.add(2);
		shepherd.add(2);
		shepherd.add(1);
		shepherd.add(3);
		shepherd.add(3);
		shepherd.add(3);
		shepherd.add(2);
		shepherd.add(2);

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

		ShepherdSync<Object, Integer, List<Integer>> shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector,
						new NoDuplicatesRule())
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new DiscardExpiredSlidingRule(Duration.ofMillis(50), false))
				.buildSync();

		//2 repeated element in the same window
		long instant = System.currentTimeMillis();
		shepherd.add(1, instant - 1000L);
		shepherd.add(1, instant - 999L);

		shepherd.checkWindows(); //trigger window checker execution

		shepherd.add(1, instant);

		shepherd.checkWindows(); //trigger window checker execution

		assertEquals(2, listCollector.size());
		List<List<Integer>> result = listCollector.get();
		assertEquals(1, result.get(0).get(0).intValue());
		assertEquals(1, result.get(1).get(0).intValue());
	}

	@Test
	public void accumulateSlidingWindows() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new SimpleKeyExtractor(),
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(100), false))
				.buildSync();

		long instant = System.currentTimeMillis() - 1000;
		shepherd.add("lele", instant);
		shepherd.add("lolo", instant);
		shepherd.add("lala", instant);

		shepherd.checkWindows();

		assertEquals(3, listCollector.size());
		List<List<String>> result = listCollector.get();
		assertEquals("lala", result.get(0).get(0));
		assertEquals("lolo", result.get(1).get(0));
		assertEquals("lele", result.get(2).get(0));
	}

	@Test
	public void accumulateContentSlidingWindowsInFile() throws Exception {

		FileCollector fileCollector = new FileCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						fileCollector,
						new AccumulateRule())
				.threads(1)
				.withValuesStorageProvider(FileValuesStorage::new)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(1000), false))
				.buildSync();

		long instant = System.currentTimeMillis() - 1100;
		shepherd.add("lolo", instant);
		shepherd.add("lala", instant + 1);
		shepherd.add("lele", instant + 55);

		shepherd.checkWindows();

		List<Path> result = fileCollector.get();
		assertEquals(1, result.size());

		assertEquals("lololalalele", new String(Files.readAllBytes(result.get(0))));

		//release resources
		deleteDir();
	}

	@Test
	public void accumulateContentSlidingWindowsInRedis() throws Exception {

		ListCollector binaryCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						binaryCollector,
						new AccumulateRule())
				.threads(1)
				.withValuesStorageProvider(RedisValuesStorage::new)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(1000), false))
				.buildSync();

		long instant = System.currentTimeMillis() - 1100;
		shepherd.add("lolo", instant);
		shepherd.add("lala", instant + 55);
		shepherd.add("lele", instant + 888);

		shepherd.checkWindows();

		List<String> result = binaryCollector.get();
		assertEquals(1, result.size());

		assertEquals("lololalalele", result.get(0));

		//clean redis
		cleanRedis(FixedKeyExtractor.KEY);
	}

	@Test
	public void accumulateFixedWindowInMemory() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredTumblingWindowRule(Duration.ofMillis(100)))
				.buildSync();

		shepherd.add("lolo", 0);
		shepherd.add("lala", 10);
		shepherd.add("lele", 110);

		shepherd.checkWindows();

		List<List<String>> result = listCollector.get();
		assertEquals(2, result.size());
		assertEquals(1, result.get(1).size());
		assertEquals("lele", result.get(1).get(0));
		assertEquals(2, result.get(0).size());
		assertEquals("lolo", result.get(0).get(0));
		assertEquals("lala", result.get(0).get(1));
	}

	@Test
	public void accumulateMultiKeyFixedWindowInMemory() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredTumblingWindowRule(Duration.ofMillis(100)))
				.buildSync();

		shepherd.add("k", "lolo", 0);
		shepherd.add("k", "lolo", 1);
		shepherd.add("k2", "lala", 10);
		shepherd.add("k", "lele", 110);

		shepherd.checkWindows();

		List<List<String>> result = listCollector.get();
		assertEquals(3, result.size());
		assertEquals(2, result.get(0).size());
		assertEquals("lolo", result.get(0).get(0));
		assertEquals("lolo", result.get(0).get(1));
		assertEquals(1, result.get(1).size());
		assertEquals("lele", result.get(1).get(0));
		assertEquals(1, result.get(2).size());
		assertEquals("lala", result.get(2).get(0));
	}

	@Test
	public void accumulateInSessionWindow() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						listCollector)
				.threads(1)
				.withWindow(
						Duration.ofMillis(10),
						new GroupExpiredSlidingRule(Duration.ofMillis(500), true))
				.buildSync();

		long instant =  System.currentTimeMillis() - 1000;
		shepherd.add("lolo", instant);
		shepherd.add("lala", instant + 10);
		shepherd.add("lele", instant + 510);

		shepherd.checkWindows();

		List<List<String>> result = listCollector.get();
		assertTrue(result.isEmpty());

		Thread.sleep(500);

		shepherd.checkWindows();

		result = listCollector.get();
		assertEquals(1, result.size());
		assertEquals(3, result.get(0).size());
		assertEquals("lolo", result.get(0).get(0));
		assertEquals("lala", result.get(0).get(1));
		assertEquals("lele", result.get(0).get(2));
	}

	@Test
	public void sumValuesInWindow() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						listCollector,
						new SumRule())
				.threads(1)
				.withValuesStorageProvider(InMemoryValuesStorage::new)
				.withWindow(
						Duration.ofMillis(20),
						new GroupExpiredSlidingRule(Duration.ofMillis(500), false))
				.buildSync();

		shepherd.add(Long.valueOf(11), 0);
		shepherd.add(Long.valueOf(22), 10);
		shepherd.add(Long.valueOf(33), 110);

		shepherd.checkWindows();

		List<Number> result = listCollector.get();
		assertEquals(1, result.size());
		assertEquals(66L, result.get(0).longValue());
	}

	@Test
	public void avgInWindow() throws Exception {

		ListCollector listCollector = new ListCollector();

		ShepherdSync shepherd = ShepherdBuilder.create()
				.basic(
						listCollector,
						new SumRule())
				.threads(1)
				.withValuesStorageProvider(InMemoryValuesStorage::new)
				.withWindow(
						Duration.ofMillis(50),
						new AvgTumblingRule(Duration.ofMillis(200), Duration.ofMillis(100)))
				.buildSync();

		shepherd.add("k1", 10L, 0);
		shepherd.add("k2", 11L, 0);
		shepherd.add("k1", 20L, 10);
		shepherd.add("k1", 30L, 110);

		shepherd.checkWindows();

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