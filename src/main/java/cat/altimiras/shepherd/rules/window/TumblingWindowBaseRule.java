package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.RuleWindow;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixed window is not related to element added to the system, it depends on UTC.
 * Creating windows starting and closing at "o'clock"
 */
public abstract class TumblingWindowBaseRule<V, S> extends WindowBaseRule<V, S> implements RuleWindow<V, S> {

	private static final Logger log = LoggerFactory.getLogger(TumblingWindowBaseRule.class);

	final private long delayed;

	public TumblingWindowBaseRule(Duration window, Duration delayed, Clock clock) {
		super(window, clock);
		this.delayed = delayed.toMillis();
	}

	@Override
	final public boolean isSliding() {
		return false;
	}

	@Override
	final public WindowKey adaptKey(Object key, long eventTs) {
		return new WindowKey(key, calculateWindow(eventTs), clock);
	}

	private long calculateWindow(long instant) {
		long mod = instant % windowInMillis;
		return instant + windowInMillis - mod;
	}

	protected boolean isWindowOpen(Metadata<? extends WindowKey> metadata) {
		return !isWindowExpired(metadata);
	}

	protected boolean isWindowExpired(Metadata<? extends WindowKey> metadata) {
		long now = clock.millis();
		//close if window time + delayed time reached and if window has at least being opened for the delayed time,
		//second condition is to be ensure that old data has a chance to be processed
		if (now >= metadata.getKey().getWindow() + delayed && now - metadata.getKey().getCreationTime() >= delayed) {
			if (log.isDebugEnabled()) {
				log.debug("Window is still EXPIRED. Due to time reference + delayed: {}, now {}", new Date(metadata.getKey().getWindow() + delayed), new Date(now));
			}
			return true;
		}
		return false;
	}
}