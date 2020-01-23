package cat.altimiras.shepherd.storage;

import java.util.LinkedHashMap;

public class LinkedMapStorage<K, T> extends BaseMapStorage<K, T> {

	public LinkedMapStorage() {
		super(new LinkedHashMap<>());
	}


}