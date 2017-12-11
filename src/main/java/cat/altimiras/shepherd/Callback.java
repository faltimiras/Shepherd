package cat.altimiras.shepherd;

import java.util.List;
import java.util.function.Consumer;

@FunctionalInterface
public interface Callback<T> extends Consumer<List<T>> {
}
