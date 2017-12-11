package cat.altimiras.shepherd.scheduler;

public interface Scheduler {

	/**
	 * Give maximum time before dog(reaper) must be executed
	 *
	 * @return
	 */
	long calculateWaitingTime();

	/**
	 * Tells to scheduler that dog just has ran
	 */
	void justExecuted();
}
