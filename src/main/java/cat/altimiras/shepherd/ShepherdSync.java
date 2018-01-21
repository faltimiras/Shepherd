package cat.altimiras.shepherd;

import cat.altimiras.shepherd.consumer.BasicConsumer;
import cat.altimiras.shepherd.consumer.DogConsumer;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ShepherdSync<T> implements Shepherd<T>{

	protected static Logger log = Logger.getLogger(ShepherdSync.class.getSimpleName());

	private final KeyExtractor keyExtractor;

	private final Callback<T> callback;

	private final RuleExecutor<T> ruleExecutor;

	private final QueueConsumer<T> syncConsumer;

	private final boolean hasDog;


	ShepherdSync(KeyExtractor keyExtractor, List<Rule<T>> rules, RuleExecutor<T> ruleExecutor, Callback<T> callback, Optional<ShepherdBuilder.Dog> dog) {

		this.keyExtractor = keyExtractor;
		this.callback = callback;
		this.ruleExecutor = ruleExecutor;

		if (hasDog = dog.isPresent()) {
			syncConsumer = new DogConsumer(rules, this.ruleExecutor, null, null, dog.get().getRulesTimeout(), dog.get().getTtl(), Clock.systemUTC(), dog.get().getRuleExecutor(), this.callback);
		}
		else {
			syncConsumer = new BasicConsumer(rules, null, this.ruleExecutor, this.callback);
		}
	}

	public boolean add(T t, Instant timestmap) {
		try {
			Object key = keyExtractor.key(t);
			if (key == null) {
				log.log(Level.SEVERE, "Extracted key == null, discarding object");
				log.log(Level.INFO, "Element discarded {0}", t);
				return false;
			}
			else {
				Element element = timestmap == null ? new Element(key, t) : new Element(key, t, timestmap);
				syncConsumer.consume(element);
			}
			return true;
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error adding element", e);
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
			((DogConsumer)syncConsumer).checkTimeouts();
		} else {
			throw new UnsupportedOperationException("Can not force timeout if shepheard has not a dog");
		}
	}

}
