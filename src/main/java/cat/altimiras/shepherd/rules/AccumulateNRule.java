package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

public class AccumulateNRule implements Rule<Object> {

	final private int amount;

	public AccumulateNRule(int n) {
		this.amount = n;
	}

	public RuleResult canGroup(Element<Object> element) {

		if (element.getValues().size() >= amount) {
			return RuleResult.canGroup(element.getValues());
		}
		else {
			return RuleResult.cantGroup(element);
		}
	}
}
