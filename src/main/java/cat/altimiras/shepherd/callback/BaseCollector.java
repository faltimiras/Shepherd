package cat.altimiras.shepherd.callback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class BaseCollector<T> implements Consumer<T> {

	final protected LinkedBlockingDeque<T> results;

	public BaseCollector(int maxSize) {
		results = new LinkedBlockingDeque(maxSize);
	}

	public BaseCollector() {
		results = new LinkedBlockingDeque();
	}

	public List<T> get() {
		List<T> elements = new ArrayList(results.size());
		results.drainTo(elements);
		return elements;
	}

	public List<T> get(int numElements) {
		int size = results.size() > numElements ? numElements : results.size();
		List<T> elements = new ArrayList(size);
		results.drainTo(elements, size);
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
			results.add(t);
		}
	}
}