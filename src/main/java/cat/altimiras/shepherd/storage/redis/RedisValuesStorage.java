package cat.altimiras.shepherd.storage.redis;

import cat.altimiras.shepherd.storage.ValuesStorage;
import cat.altimiras.shepherd.storage.serdes.BasicSerializer;
import java.util.function.Function;
import redis.clients.jedis.Jedis;

public class RedisValuesStorage implements ValuesStorage<Object, Object, String> {

	//Serializers
	final private Function<Object, String> keySerializer;

	final private Function<Object, String> valueSerializer;

	final private Jedis jedis;

	public RedisValuesStorage() {
		this.keySerializer = new BasicSerializer();
		this.valueSerializer = new BasicSerializer();

		this.jedis = new Jedis();
	}

	public RedisValuesStorage(String host, int port, boolean ssl, String psw, Function<Object, String> keySerializer, Function<Object, String> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.jedis = new Jedis(host, port, ssl);
		this.jedis.auth(psw);
	}

	public RedisValuesStorage(String host, int port, boolean ssl, Function<Object, String> keySerializer, Function<Object, String> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.jedis = new Jedis(host, port, ssl);
	}

	@Override
	public void append(Object key, Object value) {
		jedis.append(keySerializer.apply(key), valueSerializer.apply(value));
	}

	@Override
	public void remove(Object key) {
		jedis.del(keySerializer.apply(key));
	}

	@Override
	public String get(Object key) {
		return jedis.get(keySerializer.apply(key));
	}

	@Override
	public void override(Object key, String value) {
		jedis.set(keySerializer.apply(key), value);
	}

	@Override
	public Iterable keys() {
		return jedis.keys("*");
	}

	Jedis getJedis() {
		return jedis;
	}
}