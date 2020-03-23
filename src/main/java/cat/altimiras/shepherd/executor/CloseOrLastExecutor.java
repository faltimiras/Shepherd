package cat.altimiras.shepherd.executor;


import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

/**
 * Stops at first rule that returns a canClose
 * If any rule can not group, keeps/discard/append values returned by last executed rule
 *
 * @param <T>
 */
public class CloseOrLastExecutor<T, S> implements RuleExecutor<T> {

	@Override
	public RuleResult<T> execute(Metadata metadata, T newValue, LazyValue lazyValue, List<Rule<T>> rules) {

		RuleResult result = null;
		for (Rule rule : rules) {
			result = rule.canClose(metadata, newValue, lazyValue);
			if (result.canClose()) {
				return result;
			}
		}
		return result == null ? RuleResult.notGroupAndDiscardAll() : result;
	}
}