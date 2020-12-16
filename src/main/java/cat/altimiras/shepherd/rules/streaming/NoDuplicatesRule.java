package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoDuplicatesRule implements Rule {
	private static final Logger log = LoggerFactory.getLogger(NoDuplicatesRule.class);

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValues lazyValues) {
		long present = metadata.getElementsCount();
		if (present == 0) {
			log.debug("First element {}. Grouping", value);
			return RuleResult.appendAndGroup();
		} else {
			log.debug("Repeated element {} detected.", value);
			return RuleResult.notGroup();
		}
	}
}
