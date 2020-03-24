package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

import java.util.List;

public class AccumulateRule implements Rule {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValue lazyValue) {
		return RuleResult.notGroupAndAppend();
	}
}