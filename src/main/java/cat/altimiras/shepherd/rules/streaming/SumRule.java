package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

public class SumRule implements Rule<Number, Number> {

	@Override
	public RuleResult canClose(Metadata metadata, Number value, LazyValues<?, Number, Number> lazyValues) {

		Long sum;
		Number current = lazyValues.get();
		if (current == null) {
			sum = value.longValue();
		} else {
			sum = lazyValues.get().longValue() + value.longValue();
		}
		metadata.incElementsCount(); //when what is stored is handled by user, update number of elements is user responsibility
		return RuleResult.notGroupAndKeep(sum);
	}
}
