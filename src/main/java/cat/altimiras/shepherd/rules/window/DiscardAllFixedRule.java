package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class DiscardAllFixedRule extends FixedWindowBaseRule {

	private static Logger log = LoggerFactory.getLogger(DiscardAllFixedRule.class);

	public DiscardAllFixedRule(Duration window, Duration delayed) {
		super(window, delayed, Clock.systemUTC());
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValue lazyValue) {

		log.debug("Executing DiscardAllFixedRule check");

		if (isWindowExpired(metadata)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.notGroupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}
