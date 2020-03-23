package cat.altimiras.shepherd;

import java.util.List;

public class RuleResult<V> {

	protected List<V> group; //group output
	protected List<V> toKeep; //object to keep in the values storage
	private boolean canClose;
	private int append; //-1: append first, 0: not append, 1 append at the end
	private int discard; //-1: discard before, 0 : not discard, 1 discard at the end

	RuleResult() {
	}

	private void build(boolean canGroup, int append, int discard, List<V> group, List<V> toKeep) {
		this.canClose = canGroup;
		this.group = group;
		this.toKeep = toKeep;
		this.append = append;
		this.discard = discard;
	}

	public static <V> RuleResult<V> groupAndKeep(List<V> group, List<V> toKeep) {

		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 0, group, toKeep);
		return ruleResult;
	}

	public static <V> RuleResult<V> groupAndDiscard(List<V> group) {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 1, group, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> notGroupAndKeep(List<V> toKeep) {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, 0, null, toKeep);
		return ruleResult;
	}

	public static <V> RuleResult<V> groupAndDiscardAll() {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 1, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> groupAndAppend() {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 1, 0, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> discardAndAppendAndGroup() {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, -1, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> appendAndGroup() {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, 0, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> appendAndGroupAndDiscard() {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, 1, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> groupAndDiscardAndAppend() {
		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 1, 1, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> notGroupAndAppend() {

		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 1, 0, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> notGroup() {

		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, 0, null, null);
		return ruleResult;
	}

	public static <V> RuleResult<V> notGroupAndDiscardAll() {

		RuleResult<V> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, -1, null, null);
		return ruleResult;
	}

	void reset() {
		this.canClose = false;
		this.group = null;
		this.toKeep = null;
		this.append = 0;
		this.discard = 0;
	}

	void setCanClose(boolean canClose) {
		this.canClose = canClose;
	}

	public boolean canClose() {
		return canClose;
	}

	public int getAppend() {
		return append;
	}

	void setAppend(int append) {
		this.append = append;
	}

	public List<V> getGroup() {
		return group;
	}

	void setGroup(List<V> group) {
		this.group = group;
	}

	public List<V> getToKeep() {
		return toKeep;
	}

	void setToKeep(List<V> toKeep) {
		this.toKeep = toKeep;
	}

	public int getDiscard() {
		return discard;
	}

	public void setDiscard(int discard) {
		this.discard = discard;
	}
}