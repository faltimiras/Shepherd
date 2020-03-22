package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.RuleResult;

public class GroupAllRule implements Rule<Object> {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValue lazyValue) {
		return RuleResult.groupAllAndDiscard();
	}
}
