package cat.altimiras.shepherd;

import java.util.List;

public class RuleResult<T> {

	private boolean canGroup;
	protected List<T> group;
	protected Element<T> toKeep;


	private RuleResult(boolean canGroup, List<T> group, Element<T> toKeep) {
		this.canGroup = canGroup;
		this.group = group;
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
		return new RuleResult(true, group, toKeep);
	}

	public static RuleResult canGroup(List group) {
		return new RuleResult(true, group, null);
	}

	public static RuleResult cantGroup(Element toKeep) {
		return new RuleResult(false, null, toKeep);
	}

	public static RuleResult cantGroup() {
		return new RuleResult(false, null, null);
	}

	public static RuleResult canNotGroup(Element toKeep) {
		if (toKeep == null || toKeep.getValues() == null || toKeep.getValues().isEmpty()){
			return new RuleResult(false, null, null);
		}
		return new RuleResult(false, null, toKeep);
	}

	public static RuleResult canNotGroup() {
		return new RuleResult(false, null, null);
	}


}
