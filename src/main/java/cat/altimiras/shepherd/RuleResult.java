package cat.altimiras.shepherd;

import java.util.List;

public class RuleResult<T> {

	private boolean canGroup;
	protected List<T> group;
	protected Element<T> toKeep;

	RuleResult() {
	}

	private void build(boolean canGroup, List<T> group, Element<T> toKeep) {
		this.canGroup = canGroup;
		this.group = group;
		this.toKeep = toKeep;
	}

	void reset() {
		this.canGroup = false;
		this.group = null;
		toKeep = null;
	}

	void setCanGroup(boolean canGroup) {
		this.canGroup = canGroup;
	}

	void setGroup(List<T> group) {
		this.group = group;
	}

	void setToKeep(Element<T> toKeep) {
		this.toKeep = toKeep;
	}

	public boolean canGroup() {
		return canGroup;
	}

	public List<T> getGroup() {
		return group;
	}

	public Element<T> getToKeep() {
		return toKeep;
	}

	public static RuleResult canGroup(List group, Element toKeep) {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, group, toKeep);
		return ruleResult;
	}

	public static RuleResult canGroup(List group) {
		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(true, group, null);
		return ruleResult;
	}

	public static RuleResult cantGroup(Element toKeep) {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, null, toKeep);
		return ruleResult;
	}

	public static RuleResult cantGroup() {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, null, null);
		return ruleResult;
	}

	public static RuleResult canNotGroup(Element toKeep) {
		RuleResult ruleResult = RuleResultPool.borrow();

		if (toKeep == null || toKeep.getValues() == null || toKeep.getValues().isEmpty()) {
			ruleResult.build(false, null, null);
		}
		else {
			ruleResult.build(false, null, toKeep);
		}
		return ruleResult;

	}

	public static RuleResult canNotGroup() {

		RuleResult ruleResult = RuleResultPool.borrow();
		ruleResult.build(false, null, null);
		return ruleResult;
	}
}