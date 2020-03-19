package cat.altimiras.shepherd.storage.file;

import cat.altimiras.shepherd.storage.serdes.BasicBinarySerializer;
import cat.altimiras.shepherd.storage.serdes.BasicSerializer;
import cat.altimiras.shepherd.storage.ValuesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * Store values in the file system.
 *
 * This storage NEVER remove the file created, it is user responsability do something with it
 *
 * @param <K>
 * @param <V>
 */
public class FileValuesStorage<K, V> implements ValuesStorage<K, V, Path> {

	protected static Logger log = LoggerFactory.getLogger(FileValuesStorage.class);

	final private Function<Object, byte[]> serializer;
	final private Path storageBaseDir;
	final private byte[] separator;

	private Map<K, Path> storage = new HashMap<>();

	public FileValuesStorage(Path storageBaseDir, Function serializer, byte[] separator){
		this.storageBaseDir = storageBaseDir;
		this.serializer = serializer;
		this.separator = separator;

		try {
			Files.createDirectories(this.storageBaseDir);
		} catch (Exception e){
			log.error("Error creating dir structure ", e);
		}
	}

	public FileValuesStorage() {
		this.storageBaseDir = Paths.get(System.getProperty("java.io.tmpdir"), "shepherd", UUID.randomUUID().toString());
		this.serializer = new BasicBinarySerializer();
		this.separator = new byte[]{};
		try {
			Files.createDirectories(this.storageBaseDir);
		} catch (Exception e){
			log.error("Error creating dir structure ", e);
		}
	}

	@Override
	public void append(K key, V value) {

		Path path = storage.get(key);
		try {
			if (path == null) {
				log.debug("Key {} not present", key);
				path = storageBaseDir.resolve(key.toString() + UUID.randomUUID().toString());
				Files.createFile(path);
				log.debug("New path {} for key {} created", path, key);
				storage.put(key, path);
			}

			Files.write(path, serializer.apply(value), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			Files.write(path, separator, StandardOpenOption.APPEND);
			log.debug("Value appended into the file {}", path);
		} catch (Exception e) {
			log.error("Error appending value to the file {} ", e);
			throw new RuntimeException("Error storing value", e);
		}
	}

	@Override
	public Path drain(K key) {
		return storage.remove(key);
	}

	@Override
	public void override(K key, List<V> value) {

		Path path = storage.get(key);
		try {
			if (path == null) {
				log.debug("Path {} not present", path);
				path = storageBaseDir.resolve(key.toString());
				log.debug("New path {} for key {} created", path, key);
				storage.put(key, path);
			} else {
				Files.deleteIfExists(path);
			}

			for (V v : value) {
				Files.write(path, serializer.apply(v), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				Files.write(path, separator, StandardOpenOption.APPEND);
			}
		} catch (Exception e) {
			log.error("Error appending value to the file {} ", e);
			throw new RuntimeException("Error storing value", e);
		}
	}

	@Override
	public void remove(K key) {
		storage.remove(key);
	}

	@Override
	public List<V> get(K key) {
		throw new UnsupportedOperationException("File storage do not support it. Hint: If you needed you can code your own FileStorage");
	}

	@Override
	public Path publish(K key) {
		return storage.get(key);
	}
}