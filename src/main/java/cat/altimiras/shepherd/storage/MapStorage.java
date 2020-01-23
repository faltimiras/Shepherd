package cat.altimiras.shepherd.storage;

import java.util.HashMap;

public class MapStorage<K, T> extends BaseMapStorage<K, T> {

	public MapStorage() {
		super(new HashMap<>());
	}
}