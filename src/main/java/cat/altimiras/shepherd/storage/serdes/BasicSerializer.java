package cat.altimiras.shepherd.storage.serdes;

import java.util.function.Function;

public class BasicSerializer implements Function<Object, String> {

	@Override
	public String apply(Object o) {
		return o.toString();
	}
}
