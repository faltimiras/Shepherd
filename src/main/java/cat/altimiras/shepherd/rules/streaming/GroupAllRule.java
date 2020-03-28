package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

public class GroupAllRule implements Rule {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValues lazyValues) {
		return RuleResult.groupAndDiscardAll();
	}
}
