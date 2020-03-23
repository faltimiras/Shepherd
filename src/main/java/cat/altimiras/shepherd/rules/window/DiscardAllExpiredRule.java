package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValue;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.QueueConsumer;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class DiscardAllExpiredRule extends SlidingWindowBaseRule {

	private static Logger log = LoggerFactory.getLogger(DiscardAllExpiredRule.class);

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

		log.debug("Executing DiscardAllExpireRule check");

		if (isWindowExpired(metadata, fromLastElement)) {
			log.debug("Windows is expired, closing it");
			return RuleResult.notGroupAndDiscardAll();
		}
		return RuleResult.notGroup();
	}
}