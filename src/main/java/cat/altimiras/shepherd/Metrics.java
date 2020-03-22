package cat.altimiras.shepherd;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

public class Metrics {

	private final MetricRegistry metricRegistry;
	private final Counter pendingElementToProcess;
	private final Timer execTimeRules;
	private final Timer execTimeTimeout;
	private final AutoCloseable emptyContext = new DevNullAutoClosable();

	public Metrics(MetricRegistry metricRegistry){

		if (metricRegistry != null){
			this.metricRegistry = metricRegistry;
			this.pendingElementToProcess = metricRegistry.counter(name(Shepherd.class, "pending-obj"));
			this.execTimeRules = metricRegistry.timer(name(Shepherd.class, "exec-rules"));
			this.execTimeTimeout = metricRegistry.timer(name(Shepherd.class, "exec-timeout-rules"));
		}
		else {
			this.metricRegistry = null;
			this.pendingElementToProcess = null;
			this.execTimeRules= null;
			this.execTimeTimeout=null;
		}
	}

	public void pendingInc(){
		if (pendingElementToProcess !=null){
			pendingElementToProcess.inc();
		}
	}

	public void pendingDec(){
		if (pendingElementToProcess !=null){
			pendingElementToProcess.dec();
		}
	}

	public AutoCloseable rulesExecTime(){
		if (execTimeRules == null){
			return emptyContext;
		}
		return execTimeRules.time();
	}

	public AutoCloseable rulesTimeoutExecTime(){
		if (execTimeTimeout == null){
			return emptyContext;
		}
		return execTimeTimeout.time();
	}

	public double getRulesExec1minRate(){
		if (execTimeRules == null){
			return 0;
		}
		return execTimeRules.getOneMinuteRate();
	}

	public double getRulesTimeoutExec1minRate(){
		if (execTimeTimeout == null){
			return 0;
		}
		return execTimeTimeout.getOneMinuteRate();
	}

	public long getPending(){
		if (pendingElementToProcess == null){
			return 0;
		}
		return pendingElementToProcess.getCount();
	}

	private class DevNullAutoClosable implements AutoCloseable{
		@Override
		public void close() throws Exception {
			//nothing to do
		}
	}
}
