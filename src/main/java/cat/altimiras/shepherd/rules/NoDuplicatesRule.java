package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

public class NoDuplicatesRule implements Rule<Object> {

	@Override
	public RuleResult canGroup(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {

		long present = metadata.getElementsCount();
		if (present == 0) {
			return RuleResult.appendAndGroup();
		} else {
			return RuleResult.notGroup();
		}
	}
}
