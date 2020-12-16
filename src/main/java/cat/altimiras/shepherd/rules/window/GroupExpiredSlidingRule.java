package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupExpiredSlidingRule extends SlidingWindowBaseRule {

	private static final Logger log = LoggerFactory.getLogger(GroupExpiredSlidingRule.class);

	final private boolean fromLastElement;

	public GroupExpiredSlidingRule(Duration window, boolean fromLastElement) {
		super(window, Clock.systemUTC());
		this.fromLastElement = fromLastElement;
	}

	public GroupExpiredSlidingRule(Duration window, boolean fromLastElement, Clock clock) {
		super(window, clock);
		this.fromLastElement = fromLastElement;
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValues lazyValues) {

		log.debug("Executing GroupAllExpiredRule check");

		if (isWindowExpired(metadata, fromLastElement)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.groupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}