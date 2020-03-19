package cat.altimiras.shepherd.executor;


import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;

import java.util.List;

/**
 * Every rule gets same values.
 * Stops at first rule that returns a canGroup
 * If any rule can not group, keeps values returned by last executed rule
 *
 * @param <T>
 */
public class IndependentExecutor<T, S> implements RuleExecutor<T> {

	@Override
	public RuleResult<T> execute(Metadata metadata, T newValue, LazyValue lazyValue, List<Rule<T>> rules) {

		RuleResult result = null;
		for (Rule rule : rules) {
			result = rule.canGroup(metadata, newValue, lazyValue);
			if (result.canGroup()) {
				return result;
			}
		}
		return result == null ? RuleResult.notGroupAndDiscardAll() : result;
	}
}
