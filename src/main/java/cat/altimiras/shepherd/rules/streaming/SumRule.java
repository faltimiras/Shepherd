package cat.altimiras.shepherd.rules.streaming;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.Rule;

public class SumRule implements Rule<Number, Number> {

	@Override
	public RuleResult canClose(Metadata metadata, Number value, LazyValue<?, Number, Number> lazyValue) {

		Long sum;
		Number current = lazyValue.get();
		if (current == null) {
			sum = value.longValue();
		} else {
			sum = lazyValue.get().longValue() + value.longValue();
		}
		metadata.incElementsCount(); //when what is stored is handled by user, update number of elements is user responsibility
		return RuleResult.notGroupAndKeep(sum);
	}
}
