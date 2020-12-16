package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.RuleWindow;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sliding windows depends on creation or lastElement arrival, so every key has a different window.
 */
public abstract class SlidingWindowBaseRule<K, V, S> extends WindowBaseRule<V, S> implements RuleWindow<V, S> {

	private static final Logger log = LoggerFactory.getLogger(SlidingWindowBaseRule.class);

	public SlidingWindowBaseRule(Duration window, Clock clock) {
		super(window, clock);
	}

	protected boolean isWindowExpired(Metadata<K> metadata) {
		return isWindowExpired(metadata, false);
	}

	/**
	 * @param metadata
	 * @param fromLastElement true to consider windows from last element grouped, otherwise first
	 * @return
	 */
	protected boolean isWindowExpired(Metadata<K> metadata, boolean fromLastElement) {
		long timeReference = fromLastElement ? metadata.getLastElementLiveTs() : metadata.getFirstElementLiveTs();
		long now = clock.millis();
		if (now > timeReference + windowInMillis) {
			if (log.isDebugEnabled()) {
				log.debug("Window is EXPIRED. Due to time reference {}, now {}", new Date(timeReference), new Date(now));
			}
			return true;
		}
		if (log.isDebugEnabled()) {
			log.debug("Window is still OPEN. Due to time reference {}, now {}", new Date(timeReference), new Date(now));
		}
		return false;
	}

	protected boolean isWindowOpen(Metadata<K> metadata, boolean fromLastElement) {
		return !isWindowExpired(metadata, fromLastElement);
	}

	protected boolean isWindowOpen(Metadata<K> metadata) {
		return !isWindowExpired(metadata, false);
	}

	@Override
	final public boolean isSliding() {
		return true;
	}

	@Override
	public WindowKey adaptKey(Object key, long eventTs) {
		return new WindowKey(key, 0, clock);
	}
}
