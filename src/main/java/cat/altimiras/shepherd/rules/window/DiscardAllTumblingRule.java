package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class DiscardAllTumblingRule extends TumblingWindowBaseRule {

	private static Logger log = LoggerFactory.getLogger(DiscardAllTumblingRule.class);

	public DiscardAllTumblingRule(Duration window, Duration delayed) {
		super(window, delayed, Clock.systemUTC());
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
