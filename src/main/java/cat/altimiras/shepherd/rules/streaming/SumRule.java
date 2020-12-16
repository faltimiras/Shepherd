package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SumRule implements Rule<Number, Number> {

	private static final Logger log = LoggerFactory.getLogger(SumRule.class);

	@Override
	public RuleResult canClose(Metadata metadata, Number value, LazyValues<?, Number, Number> lazyValues) {
		log.debug("Element {} received", value);
		Long sum;
		Number current = lazyValues.get();
		if (current == null) {
			sum = value.longValue();
			log.debug("First value. Current sum {}", sum);
		} else {
			sum = lazyValues.get().longValue() + value.longValue();
			log.debug("Adding {} value. Current sum {}", value.longValue(), sum);
		}
		metadata.incElementsCount(); //when what is stored is handled by user, update number of elements is user responsibility
		log.debug("Keeping temporal sum and not grouping");
		return RuleResult.notGroupAndKeep(sum);
	}
}
