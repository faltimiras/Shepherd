package cat.altimiras.shepherd.executor;


import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

/**
 * Stops at first rule that returns a canClose
 * If any rule can not group, keeps/discard/append values returned by last executed rule
 */
public class CloseOrLastExecutor<V, S> implements RuleExecutor<V, S> {

	@Override
	public RuleResult<S> execute(Metadata metadata, V newValue, LazyValues lazyValues, List<Rule<V, S>> rules) {

		RuleResult result = null;
		for (Rule rule : rules) {
			result = rule.canClose(metadata, newValue, lazyValues);
			if (result.canClose()) {
				return result;
			}
		}
		return result == null ? RuleResult.notGroupAndDiscardAll() : result;
	}
}
