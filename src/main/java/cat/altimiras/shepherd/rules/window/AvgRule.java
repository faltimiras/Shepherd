package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

public class AvgRule extends TumblingWindowBaseRule<Number, Number> {

	private static Logger log = LoggerFactory.getLogger(GroupAllExpiredRule.class);

	public AvgRule(Duration window, Duration delay, Clock clock) {
		super(window, delay, clock);
	}

	public AvgRule(Duration window) {
		super(window, Duration.ofMillis(0), Clock.systemUTC());
	}

	public AvgRule(Duration window, Duration delay) {
		super(window, delay, Clock.systemUTC());
	}

	@Override
	public RuleResult canClose(Metadata metadata, LazyValues<?, Number, Number> lazyValues) {

		if (isWindowExpired(metadata)) {
			log.debug("Windows is expired, calculating avg and closing it");
			long count = metadata.getElementsCount();
			if (count == 0) {
				return RuleResult.groupAndDiscard(0);
			} else {
				return RuleResult.groupAndDiscard(lazyValues.get().doubleValue() / count);
			}
		}
		return RuleResult.notGroup();
	}
}
