package cat.altimiras.shepherd;

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RuleResultPool<V> {

	final private static ConcurrentLinkedQueue<SoftReference<RuleResult>> pool = new ConcurrentLinkedQueue<>();

	private RuleResultPool() {
	}

	public static <V> RuleResult<V> borrow() {
		SoftReference<RuleResult> r = pool.poll();
		if (r == null) {
			return new RuleResult<V>();
		}
		return r.get();
	}


	public static void release(RuleResult ruleResult) {
		ruleResult.reset();
		pool.offer(new SoftReference<>(ruleResult));
	}
}