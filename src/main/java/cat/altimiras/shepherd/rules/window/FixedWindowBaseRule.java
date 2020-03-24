package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.rules.RuleWindow;

import java.time.Clock;
import java.time.Duration;

/**
 * Fixed window is not related to element added to the system, it depends on UTC.
 * Creating windows starting and closing at "o'clock"
 */
public abstract class FixedWindowBaseRule<V,S> extends WindowBaseRule<V,S> implements RuleWindow<V,S> {

	final private long delayed;

	public FixedWindowBaseRule(Duration window, Duration delayed, Clock clock) {
		super(window, clock);
		this.delayed = delayed.toMillis();
	}

	@Override
	final public boolean isSliding() {
		return false;
	}

	@Override
	final public WindowKey adaptKey(Object key, long eventTs) {
		return new WindowKey(key, calculateWindow(eventTs));
	}

	protected boolean isWindowOpen(Metadata<? extends  WindowKey> metadata) {
		return !isWindowExpired(metadata);
	}

	protected boolean isWindowExpired(Metadata<? extends  WindowKey> metadata) {

		long now = clock.millis();

		if (now >= metadata.getKey().getWindow()) {
			return true;
		}
		return false;
	}

	private long calculateWindow(long instant) {
		long mod = instant % windowInMillis;
		return instant + windowInMillis - mod;
	}
}