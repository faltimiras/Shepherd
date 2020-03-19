package cat.altimiras.shepherd;

public interface Rule<T> {

	RuleResult canGroup(Metadata metadata, T value, LazyValue<?, T> lazyValue);
}
