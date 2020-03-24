package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

public interface Rule<V, S> {

	RuleResult canClose(Metadata metadata, V value, LazyValue<?, V, S> lazyValue);
}
