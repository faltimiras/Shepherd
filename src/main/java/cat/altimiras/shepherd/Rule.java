package cat.altimiras.shepherd;

public interface Rule<T> {

	RuleResult canClose(Metadata metadata, T value, LazyValue<?, T> lazyValue);
}
