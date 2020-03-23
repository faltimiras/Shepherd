package cat.altimiras.shepherd.rules.keyextractors;

import java.util.function.Function;

public class FixedKeyExtractor implements Function<Object, String> {

	final public static String KEY = "k";

	@Override
	public String apply(Object o) {
		return KEY;
	}
}
