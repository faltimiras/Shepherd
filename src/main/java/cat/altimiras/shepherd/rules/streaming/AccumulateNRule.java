package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumulateNRule implements Rule<Object, List<Object>> {

	private static final Logger log = LoggerFactory.getLogger(AccumulateNRule.class);

	final private int limit;

	public AccumulateNRule(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Number of accumulated elements must be bigger than 1");
		}
		this.limit = n;
	}

	@Override
	public RuleResult canClose(Metadata metadata, Object value, LazyValues<?, Object, List<Object>> lazyValues) {
		long count = metadata.getElementsCount();
		if (count == 0) {
			log.debug("First element {} in the group. Appending it", value);
			return RuleResult.notGroupAndAppend();
		} else {
			if (count == limit - 1) {
				log.debug("Last element {} in the group before reaching the limit. Appending it and grouping", value);
				return RuleResult.appendAndGroupAndDiscard();
			} else {
				log.debug("Adding element {} in the group.", value);
				return RuleResult.notGroupAndAppend();
			}
		}
	}
}