package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class GroupAllFixedWindowRule extends FixedWindowBaseRule {

	private static Logger log = LoggerFactory.getLogger(GroupAllFixedWindowRule.class);

	GroupAllFixedWindowRule(Duration window, Clock clock) {
		super(window, clock);
	}

	public GroupAllFixedWindowRule(Duration window) {
		super(window, Clock.systemUTC());
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValue<Object, Object> lazyValue) {

		log.debug("Executing GroupAllFixedWindowRule check");

		if (isWindowExpired()) {
			log.debug("Windows is expired, closing it");
			return RuleResult.groupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}