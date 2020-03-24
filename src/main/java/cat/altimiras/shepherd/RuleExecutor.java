package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

public interface RuleExecutor<V, S> {

	RuleResult<S> execute(final Metadata metadata, final V newValue, LazyValue lazyValue, List<Rule<V, S>> rules);
}
