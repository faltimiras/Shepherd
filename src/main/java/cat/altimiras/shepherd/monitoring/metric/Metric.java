package cat.altimiras.shepherd.monitoring.metric;

public interface Metric<T> {

	Metric<T> merge(Metric<T> otherMetric);

	T get();
}
