package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

public class AccumulateRule implements Rule<Object> {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {
		return RuleResult.notGroupAndAppend();
	}
}