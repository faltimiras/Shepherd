package cat.altimiras.shepherd;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class Metrics {

	private final MetricRegistry metricRegistry;

	private final Counter pendingElementToProcess;

	private final Timer execTimeRules;

	private final Timer execTimeWindow;

	private final AutoCloseable emptyContext = new DevNullAutoClosable();

	public Metrics(MetricRegistry metricRegistry) {

		if (metricRegistry != null) {
			this.metricRegistry = metricRegistry;
			this.pendingElementToProcess = metricRegistry.counter(name("shepherd", "pending-obj"));
			this.execTimeRules = metricRegistry.timer(name("shepherd", "exec-rules"));
			this.execTimeWindow = metricRegistry.timer(name("shepherd", "exec-window-rules"));
		} else {
			this.metricRegistry = null;
			this.pendingElementToProcess = null;
			this.execTimeRules = null;
			this.execTimeWindow = null;
		}
	}

	public void pendingInc() {
		if (pendingElementToProcess != null) {
			pendingElementToProcess.inc();
		}
	}

	public void pendingDec() {
		if (pendingElementToProcess != null) {
			pendingElementToProcess.dec();
		}
	}

	public AutoCloseable rulesExecTime() {
		if (execTimeRules == null) {
			return emptyContext;
		}
		return execTimeRules.time();
	}

	public AutoCloseable ruleWindowExecTime() {
		if (execTimeWindow == null) {
			return emptyContext;
		}
		return execTimeWindow.time();
	}

	public double getRulesExec1minRate() {
		if (execTimeRules == null) {
			return 0;
		}
		return execTimeRules.getOneMinuteRate();
	}

	public double getWindowExec1minRate() {
		if (execTimeWindow == null) {
			return 0;
		}
		return execTimeWindow.getOneMinuteRate();
	}

	public long getPending() {
		if (pendingElementToProcess == null) {
			return 0;
		}
		return pendingElementToProcess.getCount();
	}

	private class DevNullAutoClosable implements AutoCloseable {
		@Override
		public void close() throws Exception {
			//nothing to do
		}
	}
}
