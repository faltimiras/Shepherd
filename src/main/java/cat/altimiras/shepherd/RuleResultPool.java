package cat.altimiras.shepherd;

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RuleResultPool {

	private static ConcurrentLinkedQueue<SoftReference<RuleResult>> pool = new ConcurrentLinkedQueue<>();

	private RuleResultPool() {
	}

	public static RuleResult borrow() {
		SoftReference<RuleResult> r = pool.poll();
		if (r == null) {
			return new RuleResult();
		}
		return r.get();
	}


	public static void release(RuleResult ruleResult) {
		ruleResult.reset();
		pool.offer(new SoftReference<>(ruleResult));
	}
}