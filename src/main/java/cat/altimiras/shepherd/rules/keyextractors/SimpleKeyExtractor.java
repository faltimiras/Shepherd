package cat.altimiras.shepherd.rules.keyextractors;

import java.util.function.Function;

public class SimpleKeyExtractor implements Function<Object, Object> {

	@Override
	public Object apply(Object o) {
		return o;
	}
}
