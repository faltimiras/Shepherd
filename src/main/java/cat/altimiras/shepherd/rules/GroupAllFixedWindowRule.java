package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

import java.time.Clock;
import java.time.Duration;

public class GroupAllFixedWindowRule extends FixedWindowBaseRule {

	GroupAllFixedWindowRule(Duration window, Clock clock) {
		super(window, clock);
	}

	public GroupAllFixedWindowRule(Duration window) {
		super(window, Clock.systemUTC());
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValue<Object, Object> lazyValue) {

		if (isWindowExpired()) {
			return RuleResult.groupAllAndDiscard();
		}
		return RuleResult.notGroup();
	}
}