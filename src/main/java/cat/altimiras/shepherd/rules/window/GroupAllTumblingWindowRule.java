package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class GroupAllTumblingWindowRule extends TumblingWindowBaseRule {

	private static Logger log = LoggerFactory.getLogger(GroupAllTumblingWindowRule.class);

	GroupAllTumblingWindowRule(Duration window, Duration delay, Clock clock) {
		super(window, delay, clock);
	}

	public GroupAllTumblingWindowRule(Duration window, Duration delay) {
		super(window, delay, Clock.systemUTC());
	}

	public GroupAllTumblingWindowRule(Duration window) {
		super(window, Duration.ofMillis(0), Clock.systemUTC());
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValue lazyValue) {

		log.debug("Executing GroupAllFixedWindowRule check");

		if (isWindowExpired(metadata)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.groupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}