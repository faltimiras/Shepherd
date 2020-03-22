package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.RuleWindow;

import java.time.Clock;
import java.time.Duration;

public abstract class WindowBaseRule implements RuleWindow<Object> {

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