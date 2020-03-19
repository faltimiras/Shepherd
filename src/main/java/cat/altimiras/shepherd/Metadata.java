package cat.altimiras.shepherd;

import java.util.HashMap;
import java.util.Map;

public class Metadata<K> {

	private final K key;
	private final long creationTs;
	private final Map<String, Object> metadata = new HashMap<>(5);
	private long lastElementTs;
	private long elementsCount; //user is responsible to keep this value updated

	public Metadata(K key, long creationTs) {
		this.key = key;
		this.creationTs = creationTs;
	}

	public K getKey() {
		return key;
	}

	public long getCreationTs() {
		return creationTs;
	}

	public long getLastElementTs() {
		return lastElementTs;
	}

	public void setLastElementTs(long lastElementTs) {
		this.lastElementTs = lastElementTs;
	}

	public void add(String key, Object value){
		this.metadata.put(key, value);
	}

	public void remove(String key){
		this.metadata.remove(key);
	}

	public Object get(String key){
		return this.metadata.get(key);
	}

	public long getElementsCount() {
		return elementsCount;
	}

	public void setElementsCount(long elementsCount) {
		this.elementsCount = elementsCount;
	}

	public void incElementsCount(){
		this.elementsCount++;
	}

	public void decElmentsCount(){
		this.elementsCount--;
	}

	public void incElementsCount(long value){
		this.elementsCount = this.elementsCount + value;
	}

	public void decElmentsCount(long value){
		this.elementsCount = this.elementsCount - value;
	}
}
