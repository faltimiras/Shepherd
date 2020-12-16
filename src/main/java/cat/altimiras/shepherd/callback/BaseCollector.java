package cat.altimiras.shepherd.callback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseCollector<T> implements Consumer<T> {

	private static final Logger log = LoggerFactory.getLogger(BaseCollector.class);

	final protected LinkedBlockingDeque<T> results;

	public BaseCollector(int maxSize) {
		results = new LinkedBlockingDeque(maxSize);
	}

	public BaseCollector() {
		results = new LinkedBlockingDeque();
	}

	/**
	 * Returns all elements in the collector.
	 * WARNING: This method is not IDEMPOTENT
	 * @return
	 */
	public List<T> get() {
		List<T> elements = new ArrayList(results.size());
		results.drainTo(elements);
		return elements;
	}


	/**
	 * Returns numElements elements in the collector.
	 * WARNING: This method is not IDEMPOTENT
	 * @param numElements
	 * @return
	 */
	public List<T> get(int numElements) {
		int size = results.size() > numElements ? numElements : results.size();
		List<T> elements = new ArrayList(size);
		results.drainTo(elements, size);
		log.debug("Return {} elements. Pending elements available to read: {}", size, results.size());
		return elements;
	}

	public int size() {
		return results.size();
	}

	public boolean isEmpty() {
		return results.isEmpty();
	}

	@Override
	public void accept(T t) {
		if (t != null) {
			log.debug("Element {} added", t);
			results.add(t);
		}
	}
}