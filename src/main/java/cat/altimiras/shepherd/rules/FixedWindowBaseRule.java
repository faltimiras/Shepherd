package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.Rule;

import java.time.Clock;
import java.time.Duration;

/**
 * Fixed window is not related to element added to the system, it depends on UTC.
 * Creating windows starting and closing at "o'clock"
 */
public abstract class FixedWindowBaseRule implements Rule<Object> {

	final private Duration window;
	final private Clock clock;
	private long endCurrentOpenWindow;

	public FixedWindowBaseRule(Duration window, Clock clock) {
		this.window = window;
		this.clock = clock;
		endCurrentOpenWindow = nextEnd();
	}

	protected boolean isWindowExpired() {

		long now = clock.millis();
		if (now >= endCurrentOpenWindow) {
			endCurrentOpenWindow = nextEnd();
			return true;
		}
		return false;
	}

	protected boolean isWindowOpen() {
		return !isWindowExpired();
	}

	long getEndCurrentOpenWindow() {
		return endCurrentOpenWindow;
	}

	private long nextEnd() {

		long now = clock.millis();
		long millis = window.toMillis();
		long mod = now % millis;
		return now + millis - mod;
	}
}