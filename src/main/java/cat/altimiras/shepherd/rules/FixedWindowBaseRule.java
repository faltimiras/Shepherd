package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.Rule;

import java.time.Clock;

/**
 * Fixed window is not related to element added to the system, it depends on UTC.
 * Creating windows starting and closing at "o'clock"
 */
public abstract class FixedWindowBaseRule implements Rule<Object> {

	final private long window;
	final private Clock clock;

	public FixedWindowBaseRule(long window, Clock clock) {
		this.window = window;
		this.clock = clock;
	}

	/**
	 * @param metadata
	 * @return
	 */
	protected boolean isWindowExpired(Metadata metadata) {

		
		long timeReference = fromLastElement ? metadata.getLastElementTs() : metadata.getCreationTs();
		long now = clock.millis();
		if (now > timeReference + window) {
			return true;
		}
		return false;
	}


	protected boolean isWindowOpen(Metadata metadata) {
		return !isWindowExpired(metadata);
	}
}
