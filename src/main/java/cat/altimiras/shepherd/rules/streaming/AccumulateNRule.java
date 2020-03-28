package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

public class AccumulateNRule implements Rule<Object, List<Object>> {

	final private int limit;

	public AccumulateNRule(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Number of accumulated elements must be bigger than 1");
		}
		this.limit = n;
	}

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValues<?, Object, List<Object>> lazyValues) {

		long count = metadata.getElementsCount();
		if (count == 0) {
			return RuleResult.notGroupAndAppend();
		} else {
			if (count == limit - 1) {
				return RuleResult.appendAndGroupAndDiscard();
			} else {
				return RuleResult.notGroupAndAppend();
			}
		}


	}

}