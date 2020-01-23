package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

public class NoDuplicatesRule implements Rule<Object> {

	public RuleResult canGroup(Element<Object> element) {

		int size = element.getValues().size();
		if (size == 1) {
			return RuleResult.canGroup(element.getValues(), element);
		}
		else if (size == 2) {

			Element onlyOne = new Element(element.getKey(), element.getValues().get(1));

			if (element.getValues().get(0).equals(element.getValues().get(1))) {
				return RuleResult.cantGroup(onlyOne);
			}
			else {
				return RuleResult.canGroup(onlyOne.getValues(), onlyOne);
			}
		}
		else {
			throw new RuntimeException("Can not be more than 2 elements on that list. Inconsistency detected");
		}

	}
}
