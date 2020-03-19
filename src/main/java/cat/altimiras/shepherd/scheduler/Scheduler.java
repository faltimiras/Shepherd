package cat.altimiras.shepherd.scheduler;

public interface Scheduler {

	/**
	 *
	 * Give maximum time before dog(reaper) must be executed
	 *
	 * Value can be negative or positive
	 * positives: wait at most this number of ms for the next element in the queue
	 * negative: do not process more elements, just to nothing until timeout expires. Strict mode, less performant but useful if dog execution is critical
	 *
	 * @return
	 */
	long calculateWaitingTime(long lastRuleExecutionDurationMs);

	/**
	 * Tells to scheduler that dog just has ran
	 */
	void justExecuted();
}
