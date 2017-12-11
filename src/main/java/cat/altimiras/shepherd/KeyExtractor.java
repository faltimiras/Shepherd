package cat.altimiras.shepherd;

public interface KeyExtractor<T> {

	Object key(T t);
}
