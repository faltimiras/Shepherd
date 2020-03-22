package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.Rule;
import cat.altimiras.shepherd.RuleResult;

public class NoDuplicatesRule implements Rule<Object> {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {

		long present = metadata.getElementsCount();
		if (present == 0) {
			return RuleResult.appendAndGroup();
		} else {
			return RuleResult.notGroup();
		}
	}
}
