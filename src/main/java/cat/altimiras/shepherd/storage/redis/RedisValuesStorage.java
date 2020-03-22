package cat.altimiras.shepherd.storage.redis;

import cat.altimiras.shepherd.storage.ValuesStorage;
import cat.altimiras.shepherd.storage.serdes.BasicSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.function.Function;

public class RedisValuesStorage implements ValuesStorage<Object, Object, String> {

	//Serializers
	final private Function<Object, String> keySerializer;
	final private Function<Object,String> valueSerializer;

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
	public List<Object> get(Object key) {
		throw new UnsupportedOperationException("Separator not defined");
	}

	@Override
	public String drain(Object key) {
		String k = keySerializer.apply(key);
		Transaction tx = jedis.multi();
		Response<String> response = tx.get(k);
		tx.del(k);
		tx.exec();

		return response.get();
	}

	@Override
	public void override(Object key, List value) {
		jedis.set(keySerializer.apply(key), valueSerializer.apply(value));
	}

	@Override
	public String publish(Object key) {
		return jedis.get(keySerializer.apply(key));
	}

	Jedis getJedis() {
		return jedis;
	}
}