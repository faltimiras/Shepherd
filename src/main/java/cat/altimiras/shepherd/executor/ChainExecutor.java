package cat.altimiras.shepherd.executor;


import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleExecutor;
import cat.altimiras.shepherd.RuleResult;

import java.util.List;

/**
 * A rule gets element from that last rule decides to keep
 * Stops at first rule that returns a canGroup
 * If any rule can not group, keeps values returned by last executed rule
 *
 * @param <T>
 */
public class ChainExecutor<T,S> implements RuleExecutor<T> {

	//TODO
	/*
	@Override
	public RuleResult<T> execute(Record<T> record, List<Rule<T>> rules) {
		RuleResult result = null;
		for (Rule rule : rules) {
			result = rule.canGroup(result == null ? record : result.getToKeep());
			if (result.canGroup()) {
				return result;
			}
		}
		return result == null ? RuleResult.cantGroup() : result;
	}
*/
	@Override
	public RuleResult<T> execute(Metadata metadata, T newValue, LazyValue lazyValue, List<Rule<T>> rules) {

	return null ;

	}
}
