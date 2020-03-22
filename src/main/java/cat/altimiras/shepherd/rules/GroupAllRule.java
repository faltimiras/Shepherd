package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

import java.util.List;

public class GroupAllRule implements Rule<Object> {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValue lazyValue) {
		return RuleResult.groupAllAndDiscard();
	}
}
