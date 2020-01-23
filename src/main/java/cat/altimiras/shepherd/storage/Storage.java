package cat.altimiras.shepherd.storage;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.monitoring.debug.ElementDebugSerializer;
import cat.altimiras.shepherd.monitoring.Level;
import cat.altimiras.shepherd.monitoring.metric.Metric;
import cat.altimiras.shepherd.monitoring.Stats;

import java.util.Iterator;
import java.util.Map;

public interface Storage<K, V> {

	void put(K key, Element<V> element);

	void remove(K key);

	Element<V> getOrDefault(K key, Element<V> element);

	Iterator<Element<V>> values();

	Map<Stats, Metric> getStats(Level level, ElementDebugSerializer debugSerializer);

}
