package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import cat.altimiras.shepherd.rules.window.SlidingWindowBaseRule;

import java.time.Clock;
import java.time.Duration;

public class DiscardAllExpiredRule extends SlidingWindowBaseRule {

	final private boolean fromLastElement;

	public DiscardAllExpiredRule(Duration window) {
		super(window, Clock.systemUTC());
		this.fromLastElement = false;
	}

	public DiscardAllExpiredRule(Duration window, boolean fromLastElement) {
		super(window, Clock.systemUTC());
		this.fromLastElement = fromLastElement;
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValue<Object, Object> lazyValue) {

		if (isWindowExpired(metadata, fromLastElement)) {
			return RuleResult.notGroupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}
