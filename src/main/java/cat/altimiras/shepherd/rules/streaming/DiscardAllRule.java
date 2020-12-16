package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscardAllRule implements Rule {

	private static final Logger log = LoggerFactory.getLogger(DiscardAllRule.class);

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValues lazyValues) {
		log.debug("Element {} received, discarding", value);
		return RuleResult.notGroupAndDiscardAll();
	}
}
