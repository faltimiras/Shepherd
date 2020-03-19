package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

import java.time.Clock;

public class GroupAllExpiredRuleSliding extends SlidingWindowBaseRule {

	final private boolean fromLastElement;

	public GroupAllExpiredRuleSliding(long window, boolean fromLastElement) {
		super(window, Clock.systemUTC());
		this.fromLastElement = fromLastElement;
	}

	@Override
	public RuleResult canGroup(Metadata metadata, Object value, LazyValue<?, Object> lazyValue) {

		if(isWindowExpired(metadata, fromLastElement)){
			return RuleResult.groupAllAndDiscard();
		}
		return RuleResult.notGroup();
	}
}