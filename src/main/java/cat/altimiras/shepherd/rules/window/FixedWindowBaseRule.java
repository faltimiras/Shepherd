package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.rules.RuleWindow;

import java.time.Clock;
import java.time.Duration;

/**
 * Fixed window is not related to element added to the system, it depends on UTC.
 * Creating windows starting and closing at "o'clock"
 */
public abstract class FixedWindowBaseRule extends WindowBaseRule implements RuleWindow<Object> {

	private long endCurrentOpenWindow;

	public FixedWindowBaseRule(Duration window, Clock clock) {
		super(window, clock);
		this.endCurrentOpenWindow = calculateWindow(clock.millis());
	}

	private long calculateWindow(long instant) {
		long mod = instant % windowInMillis;
		return instant + windowInMillis - mod;
	}

	@Override
	final public boolean isSliding() {
		return false;
	}

	@Override
	final public WindowKey adaptKey(Object key, long eventTs) {
		return new WindowKey(key, calculateWindow(eventTs));
	}

	protected boolean isWindowOpen() {
		return !isWindowExpired();
	}

	protected boolean isWindowExpired() {

		long now = clock.millis();
		if (now >= endCurrentOpenWindow) {
			endCurrentOpenWindow = calculateWindow(clock.millis());
			return true;
		}
		return false;
	}

	long getEndCurrentOpenWindow() {
		return endCurrentOpenWindow;
	}
}