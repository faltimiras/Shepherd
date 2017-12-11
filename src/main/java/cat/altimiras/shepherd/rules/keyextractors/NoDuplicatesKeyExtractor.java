package cat.altimiras.shepherd.rules.keyextractors;


import cat.altimiras.shepherd.KeyExtractor;

public class NoDuplicatesKeyExtractor implements KeyExtractor<Object> {

	private static final String KEY = "k";

	@Override
	public Object key(Object o) {
		return KEY;
	}
}
