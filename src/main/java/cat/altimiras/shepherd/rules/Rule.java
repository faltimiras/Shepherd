package cat.altimiras.shepherd.rules;

import cat.altimiras.shepherd.LazyValues;
import cat.altimiras.shepherd.Metadata;
import cat.altimiras.shepherd.RuleResult;

public interface Rule<V, S> {

	/**
	 * Decides if with last element added, the group of elements can be closed or not
	 *
	 * @param metadata   group metadata. Every rule can add his own metadata across executions. WARNING: metadata instance is shared across all rules configured.
	 * @param value      last potential element to be added in the group (it has same key)
	 * @param lazyValues already elements present on this group.
	 * @return
	 */
	RuleResult canClose(Metadata metadata, V value, LazyValues<?, V, S> lazyValues);
}
