package cat.altimiras.shepherd;

public interface Rule<T> {

	RuleResult canGroup(Element<T> element);
}
