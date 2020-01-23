package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Element;
import cat.altimiras.shepherd.Rule;
import cat.altimiras.shepherd.RuleResult;

public class GroupAllRule implements Rule<Object> {

	@Override
	public RuleResult canGroup(Element<Object> element) {
		return RuleResult.canGroup(element.getValues());
	}
}
