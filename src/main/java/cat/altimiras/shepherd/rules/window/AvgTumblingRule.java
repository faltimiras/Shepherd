package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates the average of a sum of elements when the tumbling window is closed
 * This should work coordinated with streaming rule SumRule.
 * SumRule is adding in real time the values when they enter, AvgTumblingRule does the last calculus to provide the average
 */
public class AvgTumblingRule extends TumblingWindowBaseRule<Number, Number> {

	private static final Logger log = LoggerFactory.getLogger(GroupExpiredSlidingRule.class);

	public AvgTumblingRule(Duration window, Duration delay) {
		super(window, delay, Clock.systemUTC());
	}

	public AvgTumblingRule(Duration window, Duration delay, Clock clock) {
		super(window, delay, clock);
	}

	public AvgTumblingRule(Duration window, Clock clock) {
		super(window, Duration.ofMillis(0), clock);
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
