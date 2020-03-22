package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.window.SlidingWindowBaseRule;

import java.time.Clock;
import java.time.Duration;

public class GroupAllExpiredRule extends SlidingWindowBaseRule {

	final private boolean fromLastElement;

	GroupAllExpiredRule(Duration window, Clock clock, boolean fromLastElement) {
		super(window, clock);
		this.fromLastElement = fromLastElement;
	}

	public GroupAllExpiredRule(Duration window, boolean fromLastElement) {
		super(window, Clock.systemUTC());
		this.fromLastElement = fromLastElement;
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValue<Object, Object> lazyValue) {

		if (isWindowExpired(metadata, fromLastElement)) {
			return RuleResult.groupAllAndDiscard();
		}
		return RuleResult.notGroup();
	}
}