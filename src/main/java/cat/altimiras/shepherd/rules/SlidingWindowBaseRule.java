package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleWindow;

import java.time.Clock;
import java.time.Duration;

/**
 * Sliding windows depends on creation or lastElement arrival, so every key has a different window.
 */
public abstract class SlidingWindowBaseRule extends WindowBaseRule implements RuleWindow<Object> {

	public SlidingWindowBaseRule(Duration window, Clock clock) {
		super(window, clock);
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
			return true;
		}
		return false;
	}

	protected boolean isWindowExpired(Metadata metadata) {
		return isWindowExpired(metadata, false);
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
		return new WindowKey(key,0);
	}

}
