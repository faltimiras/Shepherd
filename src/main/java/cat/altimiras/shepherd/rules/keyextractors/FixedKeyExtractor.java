package cat.altimiras.shepherd.rules.keyextractors;

import java.util.function.Function;

public class FixedKeyExtractor implements Function<Object, String> {

	private final String key;

	public FixedKeyExtractor() {
		this.key = "k";
	}

	public FixedKeyExtractor(String key) {
		this.key = key;
	}

	@Override
	public String apply(Object o) {
		return this.key;
	}
}
