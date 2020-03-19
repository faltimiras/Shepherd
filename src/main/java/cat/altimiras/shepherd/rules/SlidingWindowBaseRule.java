package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;

import java.time.Clock;
import java.time.Duration;

/**
 * Sliding windows depends on creation or lastElement arrival, so every key has a different window.
 */
public abstract class SlidingWindowBaseRule implements Rule<Object> {

	final private long window;
	final private Clock clock;

	public SlidingWindowBaseRule(Duration window, Clock clock) {
		this.window = window.toMillis();
		this.clock = clock;
	}

	/**
	 * @param metadata
	 * @param fromLastElement true to consider windows from last element grouped, otherwise first
	 * @return
	 */
	protected boolean isWindowExpired(Metadata metadata, boolean fromLastElement) {

		long timeReference = fromLastElement ? metadata.getLastElementTs() : metadata.getCreationTs();
		long now = clock.millis();
		if (now > timeReference + window) {
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
}