package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

public interface Rule<T> {

	RuleResult canClose(Metadata metadata, T value, LazyValue<?, T> lazyValue);
}
