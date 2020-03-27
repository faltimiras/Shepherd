package cat.altimiras.shepherd.storage.redis;

import cat.altimiras.shepherd.storage.ValuesStorage;
import cat.altimiras.shepherd.storage.serdes.BasicSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.function.Function;

public class RedisListValuesStorage implements ValuesStorage<Object, Object, List<String>> {

	//Serializers
	final private Function<Object, String> keySerializer;
	final private Function<Object, String> valueSerializer;

	final private Jedis jedis;

	public RedisListValuesStorage() {
		this.keySerializer = new BasicSerializer();
		this.valueSerializer = new BasicSerializer();

		this.jedis = new Jedis();
	}

	public RedisListValuesStorage(String host, int port, boolean ssl, String psw, Function<Object, String> keySerializer, Function<Object, String> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.jedis = new Jedis(host, port, ssl);
		this.jedis.auth(psw);
	}

	public RedisListValuesStorage(String host, int port, boolean ssl, Function<Object, String> keySerializer, Function<Object, String> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.jedis = new Jedis(host, port, ssl);
	}

	@Override
	public void append(Object key, Object value) {
		jedis.lpush(keySerializer.apply(key), valueSerializer.apply(value));
	}

	@Override
	public void remove(Object key) {
		jedis.del(keySerializer.apply(key));
	}

	@Override
	public List get(Object key) {
		return jedis.lrange(keySerializer.apply(key), 0l, Long.MAX_VALUE);
	}

	@Override
	public void override(Object key, List value) {
		jedis.set(keySerializer.apply(key), valueSerializer.apply(value));
	}

	@Override
	public Iterable keys() {
		return jedis.keys("*");
	}

	Jedis getJedis() {
		return jedis;
	}
}