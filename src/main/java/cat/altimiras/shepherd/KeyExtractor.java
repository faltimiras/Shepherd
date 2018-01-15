package cat.altimiras.shepherd;

public interface KeyExtractor<T, K> {

	K key(T t) throws Exception;
}
