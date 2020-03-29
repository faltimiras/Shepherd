package cat.altimiras.shepherd;

import cat.altimiras.shepherd.callback.FileCollector;
import cat.altimiras.shepherd.rules.keyextractors.FixedKeyExtractor;
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
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;

@Ignore
public class Examples {

	/**
	 * For 5 minutes accumulates elements (just a current timestamp) in files.
	 * Each file is the accumulations of one tumbling window, so contains at most 2 minutes of data, starting at pair minute
	 *
	 * @throws Exception
	 */
	@Test
	public void accumulateElementsIn2MinTumblingWindows() throws Exception {

		FileCollector fileCollector = new FileCollector();

		Shepherd shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(), //note that all elements has same key, this is not mandatory
						fileCollector,
						new AccumulateRule())
				.threads(1)
				.withValuesStorageProvider(FileValuesStorage::new)
				.withWindow(
						Duration.ofMillis(1000),
						new GroupExpiredTumblingWindowRule(Duration.ofMinutes(2), Duration.ofMillis(0)))
				.build();

		//just do some noise to test it
		long end = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
		while (System.currentTimeMillis() < end) {
			shepherd.add(System.currentTimeMillis() + "\n");
			Thread.sleep(10000);
		}

	}

	/**
	 * Since first element received, not repeats during 30s windows
	 */
	@Test
	public void noRepeatsInSlidingWindows() throws Exception {

		Shepherd shepherd = ShepherdBuilder.create()
				.basic(
						(v) -> System.out.println("NEW GROUP: " + v),
						new NoDuplicatesRule())
				.withWindow(
						Duration.ofMillis(1000),
						new DiscardExpiredSlidingRule(Duration.ofSeconds(30)))
				.build();

		//just do some noise to test it
		long end = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();

		Random randomKeyGenerator = new Random();
		while (System.currentTimeMillis() < end) { //generating 4 types of element randomly
			int group = randomKeyGenerator.nextInt(4);
			shepherd.add(group, "Hello world " + group);
			Thread.sleep(1000);
			System.out.println("Added new element to group: " + group);
		}
	}

	/**
	 * Accumulates messages until 5 then it propagates them.
	 * Keep in mind that there is no window, so without a continuous flow of new elements, some can be stuck until 5 is reach.
	 */
	@Test
	public void accumulateInGroupsOf5() throws Exception {

		Shepherd shepherd = ShepherdBuilder.create()
				.basic(
						(v) -> System.out.println("NEW GROUP: " + v),
						new AccumulateNRule(5))
				.threads(2)
				.build();

		//just do some noise to test it
		long end = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();

		Random randomKeyGenerator = new Random();
		while (System.currentTimeMillis() < end) { //generating 4 types of element randomly
			int group = randomKeyGenerator.nextInt(4);
			shepherd.add(group, "Hello world " + group);
			Thread.sleep(1000);
			System.out.println("Added new element to group: " + group);
		}
	}

	/**
	 * Accumulates messages until 5 or 15s since first msg.
	 *
	 * @throws Exception
	 */
	@Test
	public void accumulateInGroupsOf5And30secondsMax() throws Exception {

		Shepherd shepherd = ShepherdBuilder.create()
				.basic(
						(v) -> System.out.println("NEW GROUP: " + v),
						new AccumulateNRule(5))
				.threads(2)
				.withWindow(
						Duration.ofSeconds(1),
						new GroupExpiredSlidingRule(Duration.ofSeconds(15), false)
				)
				.build();

		//just do some noise to test it
		long end = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();

		Random randomKeyGenerator = new Random();
		while (System.currentTimeMillis() < end) { //generating 4 types of element randomly
			int group = randomKeyGenerator.nextInt(4);
			shepherd.add(group, "Hello world " + group);
			Thread.sleep(1000);
			System.out.println("Added new element to group: " + group);
		}
	}

	/**
	 * Calculates the AVG of an stream of integers in a tumbling window of 15s.
	 *
	 * With an improved sumRule and avgRule more complex calculations can be done on the fly for more complex objects
	 *
	 * @throws Exception
	 */
	@Test
	public void avgInTumblingWindows() throws Exception {

		Shepherd shepherd = ShepherdBuilder.create()
				.basic(
						new FixedKeyExtractor(),
						(v) -> System.out.println("NEW AVG: " + v),
						new SumRule())
				.withValuesStorageProvider(InMemoryValuesStorage::new) //As we are accumulating a calculated value and not a group of them, a storage to store objects instead of lists is needed.
				.withWindow(
						Duration.ofSeconds(1),
						new AvgTumblingRule(Duration.ofSeconds(15))
				)
				.build();

		//just do some noise to test it
		long end = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();

		Random randomGenerator = new Random();
		while (System.currentTimeMillis() < end) {
			int value = randomGenerator.nextInt(100);
			shepherd.add(value);
			Thread.sleep(1000);
			System.out.println("Added:" + value);
		}
	}
}