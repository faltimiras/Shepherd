package cat.altimiras.shepherd.rules.keyextractors;

import java.util.function.Function;

public class SameKeyExtractor implements Function<Object, String> {

	final private static String KEY = "k";

	@Override
	public String apply(Object o) {
		return KEY;
	}
}
