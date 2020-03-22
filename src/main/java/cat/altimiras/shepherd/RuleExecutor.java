package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

public interface RuleExecutor<T> {

	RuleResult<T> execute(final Metadata metadata, final T newValue, LazyValue lazyValue, List<Rule<T>> rules);
}
