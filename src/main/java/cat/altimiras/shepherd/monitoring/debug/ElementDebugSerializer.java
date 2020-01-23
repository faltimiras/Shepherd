package cat.altimiras.shepherd.monitoring.debug;

import java.util.Map;

public interface ElementDebugSerializer<K, V> {

	Map<String,String> value(V element);

	String key(K key);
}
