package cat.altimiras.shepherd.callback;

import java.util.List;

public class ListCollector<T> extends BaseCollector<List<T>> {

	public ListCollector(int maxSize) {
		super(maxSize);
	}

	public ListCollector() {
	}
}
