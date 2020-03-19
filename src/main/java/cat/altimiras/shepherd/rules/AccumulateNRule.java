package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

import java.util.List;

public class AccumulateNRule implements Rule<Object> {

	final private int limit;

	public AccumulateNRule(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Number of accumulated elements must be bigger than 1");
		}
		this.limit = n;
	}

	@Override
	public RuleResult canGroup(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {

		long count = metadata.getElementsCount();
		if (count == 0) {
			return RuleResult.notGroupAndAppend();
		} else {
			if (count == limit -1) {
				return RuleResult.appendAndGroupAndDiscard();
			} else {
				return RuleResult.notGroupAndAppend();
			}
		}


	}
}