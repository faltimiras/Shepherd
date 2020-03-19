package cat.altimiras.shepherd;

import java.util.List;
public class RuleResult<V> {

	private boolean canGroup;
	private int append; //-1: append first, 0: not append, 1 append at the end
	private int discard; //-1: discard before, 0 : not discard, 1 discard at the end
	protected List<V> group; //group output
	protected List<V> toKeep; //object to keep in the values storage

	RuleResult() {
	}

	private void build(boolean canGroup, int append, int discard, List<V> group, List<V> toKeep) {
		this.canGroup = canGroup;
		this.group = group;
		this.toKeep = toKeep;
		this.append = append;
		this.discard = discard;
	}

	void reset() {
		this.canGroup = false;
		this.group = null;
		this.toKeep = null;
		this.append = 0;
		this.discard = 0;
	}

	void setCanGroup(boolean canGroup) {
		this.canGroup = canGroup;
	}

	void setAppend(int append) {
		this.append = append;
	}

	void setGroup(List<V> group) {
		this.group = group;
	}

	void setToKeep(List<V> toKeep) {
		this.toKeep = toKeep;
	}

	public boolean canGroup() {
		return canGroup;
	}

	public int getAppend() {
		return append;
	}

	public List<V> getGroup() {
		return group;
	}

	public List<V> getToKeep() {
		return toKeep;
	}

	public int getDiscard() {
		return discard;
	}

	public void setDiscard(int discard) {
		this.discard = discard;
	}

	public static RuleResult groupAndKeep(List<Object> group, List<Object> toKeep) {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 0, group, toKeep);
		return ruleResult;
	}

	public static RuleResult groupAndDiscard(List<Object> group) {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 1, group, null);
		return ruleResult;
	}

	public static RuleResult groupAllAndDiscard() {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 1, null, null);
		return ruleResult;
	}

	public static RuleResult groupAndAppend() {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 1, 0,null, null);
		return ruleResult;
	}

	public static RuleResult discardAndAppendAndGroup() {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, -1,null, null);
		return ruleResult;
	}

	public static RuleResult appendAndGroup() {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, 0,null, null);
		return ruleResult;
	}

	public static RuleResult appendAndGroupAndDiscard() {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, 1,null, null);
		return ruleResult;
	}

	public static RuleResult notGroupAndKeep(List<Object> toKeep) {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0,0, null, toKeep);
		return ruleResult;
	}

	public static RuleResult notGroupAndAppend() {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 1, 0,null, null);
		return ruleResult;
	}

	public static RuleResult notGroup() {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, 0,null, null);
		return ruleResult;
	}


	public static RuleResult notGroupAndDiscardAll() {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, -1,null, null);
		return ruleResult;
	}
}