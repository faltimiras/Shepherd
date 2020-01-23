package cat.altimiras.shepherd.storage;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.Stats;
import cat.altimiras.shepherd.monitoring.debug.ElementDebugSerializer;
import cat.altimiras.shepherd.monitoring.metric.AgeMetric;
import cat.altimiras.shepherd.monitoring.metric.DebugMetric;
import cat.altimiras.shepherd.monitoring.metric.Metric;
import cat.altimiras.shepherd.monitoring.metric.NumMetric;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseMapStorage<K, T> implements Storage<K, T> {

	public static final Clock clock = Clock.systemUTC();

	private Map<K, Element<T>> storage;

	public BaseMapStorage(Map storageImpl) {
		this.storage = storageImpl;
	}

	@Override
	public void put(K key, Element<T> element) {
		storage.put(key, element);
	}

	@Override
	public void remove(Object key) {
		storage.remove(key);
	}

	@Override
	public Element<T> getOrDefault(K key, Element<T> element) {
		return storage.getOrDefault(key, element);
	}

	@Override
	public Iterator<Element<T>> values() {
		return storage.values().iterator();
	}

	@Override
	public Map<Stats, Metric> getStats(Level level, ElementDebugSerializer debugSerializer) {

		long ini = clock.millis();

		Map<Stats, Metric> stats = new HashMap<>();
		stats.put(Stats.NumElements, new NumMetric(storage.size()));

		if (level.getLevel() > Level.INFO.getLevel()) {

			if (storage.isEmpty()) {
				stats.put(Stats.OldestElement, new AgeMetric(0));
				stats.put(Stats.MaxElementGroup, new NumMetric(0));
				stats.put(Stats.MinElementGroup, new NumMetric(0));
				stats.put(Stats.NumElementsTodal, new NumMetric(0));
				stats.put(Stats.AvgElementsGroup, new NumMetric(0));
				stats.put(Stats.AgeOldestElementS, new NumMetric(0));

				if (level.getLevel() == Level.TRACE.getLevel()) {
					stats.put(Stats.Elements, new DebugMetric(new HashMap<>()));
				}

			} else {
				long total = 0;
				long maxElement = 0;
				long minElement = Long.MAX_VALUE;
				Instant oldest = Instant.now();
				int currentSize;

				Map<String, List<Map<String,String>>> debugValues = new HashMap<>(storage.size());

				for (Map.Entry<K, Element<T>> e : storage.entrySet()) {
					currentSize = e.getValue().getValues().size();
					total += currentSize;
					maxElement = maxElement > currentSize ? maxElement : currentSize;
					minElement = minElement < currentSize ? minElement : currentSize;
					oldest = e.getValue().getCreationTs().isBefore(oldest) ? e.getValue().getCreationTs() : oldest;

					//extract and serialize content for debug purposes;
					if (level.getLevel() == Level.TRACE.getLevel()) {
						String key = debugSerializer.key(e.getKey());
						List<Map<String,String>> values = new ArrayList<>();
						for (Object v : e.getValue().getValues()){
							Map<String,String> serialized = debugSerializer.value(v);
							values.add(serialized);
						}

						debugValues.put(key, values);
					}
				}

				stats.put(Stats.OldestElement, new AgeMetric(oldest.toEpochMilli()));
				stats.put(Stats.AgeOldestElementS, new NumMetric((Clock.systemUTC().millis() - oldest.toEpochMilli()) / 1000));
				stats.put(Stats.MaxElementGroup, new NumMetric(maxElement));
				stats.put(Stats.MinElementGroup, new NumMetric(minElement));
				stats.put(Stats.NumElementsTodal, new NumMetric(total));
				stats.put(Stats.AvgElementsGroup, new NumMetric(storage.isEmpty() ? 0 : total / storage.size()));

				if (level.getLevel() == Level.TRACE.getLevel()) {
					stats.put(Stats.Elements, new DebugMetric(debugValues));
				}
			}
		}

		long end = clock.millis();
		stats.put(Stats.ElapsedTimeCollectingMs, new NumMetric(end - ini));

		return stats;
	}
}