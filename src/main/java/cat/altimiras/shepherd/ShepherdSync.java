package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.DogConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class ShepherdSync<T> extends ShepherdBase<T> {

	protected static final Logger log = LoggerFactory.getLogger(ShepherdSync.class);

	private final QueueConsumer<T> syncConsumer;

	private final boolean hasDog;

	ShepherdSync(KeyExtractor keyExtractor, List<Rule<T>> rules, RuleExecutor<T> ruleExecutor, Callback<T> callback, Optional<ShepherdBuilder.Dog> dog, Optional<ShepherdBuilder.Monitoring> monitoring) {

		super(keyExtractor, callback, ruleExecutor, 1, monitoring);

		if (hasDog = dog.isPresent()) {
			syncConsumer = new DogConsumer(rules, this.ruleExecutor, null, null, dog.get().getRulesTimeout(), dog.get().getTtl(), Clock.systemUTC(), dog.get().getRuleExecutor(), this.callback);
		}
		else {
			syncConsumer = new BasicConsumer(rules, null, this.ruleExecutor, this.callback);
		}

		consumers.add(syncConsumer);

	}

	public boolean add(T t, Instant timestmap) {
		try {
			Object key = keyExtractor.key(t);
			if (key == null) {
				log.error("Extracted key == null, discarding object");
				log.info("Element discarded {0}", t);
				return false;
			}
			else {
				Element element = timestmap == null ? new Element(key, t) : new Element(key, t, timestmap);
				syncConsumer.consume(element);
			}
			return true;
		}
		catch (Exception e) {
			log.error("Error adding element", e);
			return false;
		}
	}

	public boolean add(T t, Long timestmap) {
		return add(t, Instant.ofEpochMilli(timestmap));
	}

	public boolean add(T t) {
		return add(t, (Instant) null);
	}

	public void forceTimeout() {
		if (hasDog) {
			((DogConsumer) syncConsumer).checkTimeouts();
		}
		else {
			throw new UnsupportedOperationException("Can not force timeout if shepheard has not a dog");
		}
	}
}
