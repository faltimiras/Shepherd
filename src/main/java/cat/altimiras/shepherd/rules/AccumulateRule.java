package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

public class AccumulateRule implements Rule<Object> {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {
		return RuleResult.notGroupAndAppend();
	}
}