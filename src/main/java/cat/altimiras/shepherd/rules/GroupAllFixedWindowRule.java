package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

import java.time.Clock;
import java.time.Duration;

public class GroupAllFixedWindowRule extends FixedWindowBaseRule {

	final private boolean fromLastElement;

	public GroupAllFixedWindowRule(Duration window, boolean fromLastElement) {
		super(window, Clock.systemUTC());
		this.fromLastElement = fromLastElement;
	}

	@Override
	public RuleResult canGroup(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {

		if(isWindowExpired()){
			return RuleResult.groupAllAndDiscard();
		}
		return RuleResult.notGroup();
	}
}