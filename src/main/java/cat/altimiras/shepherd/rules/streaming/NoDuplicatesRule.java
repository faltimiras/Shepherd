package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

public class NoDuplicatesRule implements Rule {

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValues lazyValues) {

		long present = metadata.getElementsCount();
		if (present == 0) {
			return RuleResult.appendAndGroup();
		} else {
			return RuleResult.notGroup();
		}
	}
}
