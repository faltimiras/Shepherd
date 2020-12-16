package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupExpiredTumblingWindowRule extends TumblingWindowBaseRule {

	private static final Logger log = LoggerFactory.getLogger(GroupExpiredTumblingWindowRule.class);

	public GroupExpiredTumblingWindowRule(Duration window, Duration delay) {
		super(window, delay, Clock.systemUTC());
	}

	public GroupExpiredTumblingWindowRule(Duration window, Duration delay, Clock clock) {
		super(window, delay, clock);
	}

	public GroupExpiredTumblingWindowRule(Duration window, Clock clock) {
		super(window, Duration.ofMillis(0), clock);
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValues lazyValues) {
		log.debug("Executing GroupAllFixedWindowRule check");
		if (isWindowExpired(metadata)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.groupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}