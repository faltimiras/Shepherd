package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscardExpiredTumblingRule extends TumblingWindowBaseRule {

	private static final Logger log = LoggerFactory.getLogger(DiscardExpiredTumblingRule.class);

	public DiscardExpiredTumblingRule(Duration window, Duration delayed) {
		super(window, delayed, Clock.systemUTC());
	}

	public DiscardExpiredTumblingRule(Duration window, Duration delayed, Clock clock) {
		super(window, delayed, clock);
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValues lazyValues) {
		log.debug("Executing DiscardAllFixedRule check");
		if (isWindowExpired(metadata)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.notGroupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}
