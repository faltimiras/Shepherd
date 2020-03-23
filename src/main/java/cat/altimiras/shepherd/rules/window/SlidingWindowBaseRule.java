package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.RuleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;

/**
 * Sliding windows depends on creation or lastElement arrival, so every key has a different window.
 */
public abstract class SlidingWindowBaseRule extends WindowBaseRule implements RuleWindow<Object> {

	private static Logger log = LoggerFactory.getLogger(SlidingWindowBaseRule.class);

	public SlidingWindowBaseRule(Duration window, Clock clock) {
		super(window, clock);
	}

	protected boolean isWindowExpired(Metadata metadata) {
		return isWindowExpired(metadata, false);
	}

	/**
	 * @param metadata
	 * @param fromLastElement true to consider windows from last element grouped, otherwise first
	 * @return
	 */
	protected boolean isWindowExpired(Metadata metadata, boolean fromLastElement) {

		long timeReference = fromLastElement ? metadata.getLastElementTs() : metadata.getCreationTs();
		long now = clock.millis();
		if (now > timeReference + windowInMillis) {
			if (log.isDebugEnabled()) {
				log.debug("Window is expired. Due to time reference {}, now {}", new Date(timeReference), new Date(now));
			}
			return true;
		}
		return false;
	}

	protected boolean isWindowOpen(Metadata metadata, boolean fromLastElement) {
		return !isWindowExpired(metadata, fromLastElement);
	}

	protected boolean isWindowOpen(Metadata metadata) {
		return !isWindowExpired(metadata, false);
	}

	@Override
	final public boolean isSliding() {
		return true;
	}

	@Override
	public WindowKey adaptKey(Object key, long eventTs) {
		return new WindowKey(key, 0);
	}

}
