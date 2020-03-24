package cat.altimiras.shepherd.rules.window;

import cat.altimiras.shepherd.rules.RuleWindow;

import java.time.Clock;
import java.time.Duration;

public abstract class WindowBaseRule<V,S> implements RuleWindow<V,S> {

	final protected Duration window;
	final protected long windowInMillis;
	final protected Clock clock;

	public WindowBaseRule(Duration window, Clock clock) {
		this.window = window;
		this.windowInMillis = window.toMillis();
		this.clock = clock;
	}

	@Override
	final public Duration window() {
		return window;
	}
}