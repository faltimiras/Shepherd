package cat.altimiras.shepherd.rules.keyextractors;

import cat.altimiras.shepherd.KeyExtractor;

public class SimpleKeyExtractor implements KeyExtractor {

	@Override
	public Object key(Object o) {
		return o;
	}
}
