package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

import java.time.Duration;
import java.util.Objects;

public interface RuleWindow<V, S> {

	/**
	 * Decides if windows has to be closed or not.
	 *
	 * @param metadata   group metadata information
	 * @param lazyValues already elements present on this group.
	 * @return
	 */
	RuleResult canClose(Metadata metadata, LazyValues<?, V, S> lazyValues);

	/**
	 * @return true if it is an sliding window, false otherwise
	 */
	boolean isSliding();

	/**
	 * @return window duration
	 */
	Duration window();

	/**
	 * @param key     original key
	 * @param eventTs event timestamp
	 * @return return window key
	 */
	WindowKey adaptKey(Object key, long eventTs);

	class WindowKey {
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
		public int hashCode() {
			return Objects.hash(key, window);
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
		public String toString() {
			return "WindowKey{" +
					"key=" + key +
					", window=" + window +
					'}';
		}
	}
}
