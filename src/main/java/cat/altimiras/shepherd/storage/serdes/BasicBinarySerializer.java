package cat.altimiras.shepherd.storage.serdes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public class BasicBinarySerializer implements Function<Object, byte[]> {

	final private Charset charset;

	public BasicBinarySerializer() {
		this.charset = StandardCharsets.UTF_8;
	}

	public BasicBinarySerializer(Charset charset) {
		this.charset = charset;
	}

	@Override
	public byte[] apply(Object o) {
		if (o instanceof String) {
			return ((String) o).getBytes(charset);
		} else if (o instanceof byte[]) {
			return (byte[]) o;
		}
		throw new IllegalArgumentException("Object to serialize MUST be a String or byte[]. Hint: Implement your own serializer");
	}
}