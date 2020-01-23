package cat.altimiras.shepherd.monitoring.debug;

import java.util.HashMap;
import java.util.Map;

public class StringElementDebugSerializer implements ElementDebugSerializer {
	@Override
	public Map<String, String> value(Object element) {
		Map v = new HashMap(1);
		v.put("value", element.toString());
		return v;
	}

	@Override
	public String key(Object key) {
		return key.toString();
	}
}
