package cat.altimiras.shepherd;

import java.util.List;

public class RuleResult<S> {

	protected S group; //group output
	protected S toKeep; //object to keep in the values storage
	private boolean canClose;
	private int append; //-1: append first, 0: not append, 1 append at the end
	private int discard; //-1: discard before, 0 : not discard, 1 discard at the end

	RuleResult() {
	}

	private void build(boolean canGroup, int append, int discard, S group, S toKeep) {
		this.canClose = canGroup;
		this.group = group;
		this.toKeep = toKeep;
		this.append = append;
		this.discard = discard;
	}

	public static <S> RuleResult<S> groupAndKeep(Object group, Object toKeep) {

		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 0, (S)group, (S)toKeep);
		return ruleResult;
	}

	public static <S> RuleResult<S> groupAndDiscard(S group) {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 1, group, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> notGroupAndKeep(S toKeep) {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, 0, null, toKeep);
		return ruleResult;
	}

	public static <S> RuleResult<S> groupAndDiscardAll() {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 0, 1, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> groupAndAppend() {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 1, 0, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> discardAndAppendAndGroup() {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, -1, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> appendAndGroup() {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, 0, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> appendAndGroupAndDiscard() {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, -1, 1, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> groupAndDiscardAndAppend() {
		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, 1, 1, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> notGroupAndAppend() {

		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 1, 0, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> notGroup() {

		RuleResult<S> ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, 0, 0, null, null);
		return ruleResult;
	}

	public static <S> RuleResult<S> notGroupAndDiscardAll() {

		RuleResult<S> ruleResult = RuleResultPool.borrow();
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

	public S getGroup() {
		return group;
	}

	void setGroup(S group) {
		this.group = group;
	}

	public S getToKeep() {
		return toKeep;
	}

	void setToKeep(S toKeep) {
		this.toKeep = toKeep;
	}

	public int getDiscard() {
		return discard;
	}

	public void setDiscard(int discard) {
		this.discard = discard;
	}
}