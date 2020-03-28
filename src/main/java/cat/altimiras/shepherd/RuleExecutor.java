package cat.altimiras.shepherd;

import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

public interface RuleExecutor<V, S> {

	/**
	 * @param metadata
	 * @param newValue
	 * @param lazyValues
	 * @param rules
	 * @return
	 */
	RuleResult<S> execute(final Metadata metadata, final V newValue, LazyValues lazyValues, List<Rule<V, S>> rules);
}
