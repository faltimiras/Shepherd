package cat.altimiras.shepherd.callback;

import cat.altimiras.shepherd.Callback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class ResultsPool<T> implements Callback<T> {

	final private LinkedBlockingDeque<List<T>> results;

	public ResultsPool(int maxSize) {
		results = new LinkedBlockingDeque<List<T>>(maxSize);
	}

	public ResultsPool() {
		results = new LinkedBlockingDeque<List<T>>();
	}

	public List<List<T>> get() {
		List<List<T>> elements = new ArrayList<List<T>>(results.size());
		results.drainTo(elements);
		return elements;
	}

	public List<List<T>> get(int numElements) {
		int size = results.size() > numElements ? numElements : results.size();
		List<List<T>> elements = new ArrayList<List<T>>(size);
		results.drainTo(elements, size);
		return elements;
	}

	public int size() {
		return results.size();
	}

	@Override
	public void accept(List<T> t) {
		results.add(t);
	}
}
