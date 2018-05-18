package cat.altimiras.shepherd.monitoring;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.monitoring.metric.DateMetric;
import cat.altimiras.shepherd.monitoring.metric.Metric;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class MapExtractor {

	public static final Clock clock = Clock.systemUTC();

	public static <T> Map<Stats, Metric> extract(Map<Object, Element<T>> storage, Level level) {

		Map<Stats, Metric> stats = new HashMap<>();

		long ini = clock.millis();

		stats.put(Stats.NUM_ELEMENTS, new Metric((long) storage.size()));

		if (level.equals(Level.DEEP)) {

			if (storage.isEmpty()) {
				stats.put(Stats.OLDEST_ELEMENT, new DateMetric(0));
				stats.put(Stats.MAX_ELEMENT_GROUP, new Metric(0));
				stats.put(Stats.MIN_ELEMENT_GROUP, new Metric(0));
				stats.put(Stats.NUM_ELEMENTS_TOTAL, new Metric(0));
				stats.put(Stats.AVG_ELEMENTS_GROUP, new Metric(0));
				stats.put(Stats.AGE_OLDEST_ELEMENT_s, new Metric(0));
			}
			else {
				long total = 0;
				long maxElement = 0;
				long minElement = Long.MAX_VALUE;
				Instant oldest = Instant.now();
				int currentSize;
				for (Map.Entry<Object, Element<T>> e : storage.entrySet()) {
					currentSize = e.getValue().getValues().size();
					total += currentSize;
					maxElement = maxElement > currentSize ? maxElement : currentSize;
					minElement = minElement < currentSize ? minElement : currentSize;
					oldest = e.getValue().getCreationTs().isBefore(oldest) ? e.getValue().getCreationTs() : oldest;
				}

				stats.put(Stats.OLDEST_ELEMENT, new DateMetric(oldest.toEpochMilli()));
				stats.put(Stats.AGE_OLDEST_ELEMENT_s, new Metric((Clock.systemUTC().millis() - oldest.toEpochMilli()) / 1000));
				stats.put(Stats.MAX_ELEMENT_GROUP, new Metric(maxElement));
				stats.put(Stats.MIN_ELEMENT_GROUP, new Metric(minElement));
				stats.put(Stats.NUM_ELEMENTS_TOTAL, new Metric(total));
				stats.put(Stats.AVG_ELEMENTS_GROUP, new Metric(storage.isEmpty() ? 0 : total / storage.size()));
			}
		}

		long end = clock.millis();
		stats.put(Stats.ELAPSED_TIME_COLLECTING_ms, new Metric(end - ini));

		return stats;
	}
}