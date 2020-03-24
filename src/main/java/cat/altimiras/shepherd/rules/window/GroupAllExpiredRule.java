package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class GroupAllExpiredRule extends SlidingWindowBaseRule {

	private static Logger log = LoggerFactory.getLogger(GroupAllExpiredRule.class);

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
	public RuleResult canClose(Metadata metadata, LazyValue lazyValue) {

		log.debug("Executing GroupAllExpiredRule check");

		if (isWindowExpired(metadata, fromLastElement)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.groupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}