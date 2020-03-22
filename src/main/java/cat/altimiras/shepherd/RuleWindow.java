package cat.altimiras.shepherd;

import java.time.Duration;
import java.util.Objects;

public interface RuleWindow<V> {

	RuleResult canClose(Metadata metadata, LazyValue<Object, V> lazyValue);

	boolean isSliding();

	Duration window();

	WindowKey adaptKey(Object key, long eventTs);

	class WindowKey{
		private Object key;
		private long window;

		public WindowKey(Object key, long window) {
			this.key = key;
			this.window = window;
		}

		public Object getKey() {
			return key;
		}

		public long getWindow() {
			return window;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			WindowKey windowKey = (WindowKey) o;
			return window == windowKey.window &&
					Objects.equals(key, windowKey.key);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, window);
		}
	}
}
