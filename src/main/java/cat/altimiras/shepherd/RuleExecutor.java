package cat.altimiras.shepherd;

import java.util.List;

public interface RuleExecutor<T> {

	RuleResult<T> execute(final Element<T> elements, List<Rule<T>> rules);
}
