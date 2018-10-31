package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.Limit;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ObjectRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.Record;
import org.springframework.data.redis.connection.StreamRecords;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @since 2018/10
 */
public class ApiSpike {

	RedisClient client;
	StatefulRedisConnection<byte[], byte[]> connection;

	LettuceConnection lc;

	@Before
	public void setUp() {

		client = RedisClient.create(RedisURI.create("localhost", 6379));
		lc = new LettuceConnection(1, client);
	}

	@After
	public void tearDown() {

		lc.flushAll();
		lc.close();
		client.shutdown();
	}

	@Test
	public void all() {

		plainStuff();
		System.out.println("-----------");
		simpleObject();
		System.out.println("-----------");
		writeWithHashReadWithMap();
		System.out.println("-----------");
	}

	@Test
	public void plainStuff() {

		RedisStreamCommandsImpl imp = new RedisStreamCommandsImpl(lc);

		StreamOperationsImpl<String, String, Object> ops = new StreamOperationsImpl<>(imp, RedisSerializer.string(),
				RedisSerializer.string(), RedisSerializer.java(), null);

		RecordId id = ops.xAdd("foo", Record.of(Collections.singletonMap("field", "value")));
		List<MapRecord<String, String, Object>> range = ops.xRange("foo", Range.unbounded());

		range.forEach(it -> System.out.println(it.getId() + ": " + it.getValue()));

		List<ObjectRecord<String, String>> stringRange = ops.xRange("foo", Range.unbounded(), String.class);
		stringRange.forEach(System.out::println);
	}

	@Test
	public void simpleObject() {

		SimpleObject o = new SimpleObject();
		o.field1 = "value-1";
		o.field2 = 10L;

		RedisStreamCommandsImpl imp = new RedisStreamCommandsImpl(lc);

		StreamOperationsImpl<String, String, Object> ops = new StreamOperationsImpl<>(imp, RedisSerializer.string(),
				RedisSerializer.string(), RedisSerializer.java(), new Jackson2HashMapper(false));

		RecordId id = ops.xAdd("key", o);
		List<ObjectRecord<String, SimpleObject>> list = ops.xRange("key", Range.unbounded(), SimpleObject.class);

		list.forEach(System.out::println);
	}

	@Test
	public void writeWithHashReadWithMap() {

		SimpleObject o = new SimpleObject();
		o.field1 = "value-1";
		o.field2 = 10L;

		RedisStreamCommandsImpl imp = new RedisStreamCommandsImpl(lc);

		StreamOperationsImpl<String, String, Object> ops = new StreamOperationsImpl<>(imp, RedisSerializer.string(),
				RedisSerializer.string(), RedisSerializer.java(), null);

		RecordId id = ops.xAdd("key", o);

		List<MapRecord<String, String, Object>> list = ops.xRange("key", Range.unbounded());

		list.forEach(System.out::println);
	}

	@Data
	static class SimpleObject {

		String field1;
		Long field2;
	}

	@Test
	public void x2() {

		RedisCustomConversions rcc = new RedisCustomConversions();
		DefaultConversionService conversionService = new DefaultConversionService();
		rcc.registerConvertersIn(conversionService);

	}

	interface RedisStreamCommands {

		RecordId xAdd(byte[] key, MapRecord<byte[], byte[], byte[]> entry);

		List<MapRecord<byte[], byte[], byte[]>> xRange(byte[] key, Range<String> range);

	}

	static class RedisStreamCommandsImpl implements RedisStreamCommands {

		private LettuceConnection connection;

		public RedisStreamCommandsImpl(LettuceConnection connection) {
			this.connection = connection;
		}

		@Override
		public RecordId xAdd(byte[] key, MapRecord<byte[],byte[], byte[]> entry) {
			return RecordId
					.of(connection.getConnection().xadd(key, entry.getValue()));
		}

		@Override
		public List<MapRecord<byte[], byte[], byte[]>> xRange(byte[] key, Range<String> range) {

			List<StreamMessage<byte[], byte[]>> raw = connection.getConnection().xrange(key,
					io.lettuce.core.Range.unbounded(), Limit.unlimited());
			return raw.stream()
					.map(it -> StreamRecords.rawBytes(it.getBody())
							.withId(RecordId.of(it.getId())))
					.collect(Collectors.toList());
		}
	}

	interface StreamOperations<K, HK, HV> {

		default RecordId xAdd(K key, Object value) {
			return xAdd(key, Record.of(value));
		}

		default RecordId xAdd(K key, Record<K,?> value) {
			return xAdd(key, objectToEntry(value));
		}

		RecordId xAdd(K key, MapRecord<K, ? extends HK, ? extends HV> entry);

		List<MapRecord<K, HK, HV>> xRange(K key, Range<String> range);

		default <V> List<ObjectRecord<K, V>> xRange(K key, Range<String> range, Class<V> targetType) {
			return xRange(key, range).stream().map(it -> entryToObject(it, targetType)).collect(Collectors.toList());
		}

		default <V> MapRecord<K, HK, HV> objectToEntry(V value) {

			if (value instanceof ObjectRecord) {

				ObjectRecord entry = ((ObjectRecord) value);
				return Record.of(((HashMapper) getHashMapper(entry.getValue().getClass())).toHash(entry.getValue()))
						.withId(entry.getId());
			}

			return Record.of(((HashMapper) getHashMapper(value.getClass())).toHash(value));
		}

		default <V> ObjectRecord<K, V> entryToObject(MapRecord<K, HK, HV> entry, Class<V> targetType) {
			return entry.toObjectRecord(getHashMapper(targetType));
		}

		<V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType);
	}

	/*
	 * Conversion Rules
	 *
	 * 1) Simple types
	 *    serialize: default value serializer (key known byte array of class)
	 *    deserialized: default value serializer
	 * 2) Complex types
	 *    serialize: HashMapper: check if all entries are binary - then pass on to serializer
	 *    deserialize: deserialize then pass to hashMapper
	 *
	 *
	 *
	 *
	 */
	static class StreamOperationsImpl<K, HK, HV> implements StreamOperations<K, HK, HV> {

		private RedisSerializer<K> keySerializer;
		private RedisSerializer hashKeySerializer;
		private RedisSerializer hashValueSerializer;
		private RedisStreamCommands commands;
		private final RedisCustomConversions rcc = new RedisCustomConversions();
		private DefaultConversionService conversionService;

		private HashMapper<?, HK, HV> mapper;

		public StreamOperationsImpl(RedisStreamCommands commands, RedisSerializer<K> keySerializer,
				RedisSerializer<HK> hashKeySerializer, RedisSerializer<HV> hashValueSerializer, HashMapper<?, HK, HV> mapper) {

			this.commands = commands;
			this.keySerializer = keySerializer;
			this.hashKeySerializer = hashKeySerializer;
			this.hashValueSerializer = hashValueSerializer;

			this.conversionService = new DefaultConversionService();
			this.mapper = mapper != null ? mapper : (HashMapper<?, HK, HV>) new ObjectHashMapper();
			rcc.registerConvertersIn(conversionService);
		}

		@Override
		public RecordId xAdd(K key, MapRecord<K, ? extends HK, ? extends HV> entry) {
			return commands.xAdd(serializeKeyIfRequired(key), entry.mapEntries(this::mapToBinary).withStreamKey(serializeKeyIfRequired(key)));
		}

		@Override
		public List<MapRecord<K, HK, HV>> xRange(K key, Range<String> range) {

			return commands.xRange(serializeKeyIfRequired(key), range).stream().map(it ->it.mapEntries(this::mapToObject).withStreamKey(deserializeKey(it.getStream(), null)))
					.collect(Collectors.toList());
		}

		@Override
		public <V> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType) {

			if (rcc.isSimpleType(targetType)) {

				return new HashMapper<V, HK, HV>() {

					@Override
					public Map<HK, HV> toHash(V object) {
						return (Map<HK, HV>) Collections.singletonMap("payload".getBytes(StandardCharsets.UTF_8),
								serializeHashValueIfRequires((HV) object));
					}

					@Override
					public V fromHash(Map<HK, HV> hash) {
						Object value = hash.values().iterator().next();
						if (ClassUtils.isAssignableValue(targetType, value)) {
							return (V) value;
						}
						return (V) deserializeHashValue((byte[]) value, (Class<HV>) targetType);
					}
				};
			}

			if (mapper instanceof ObjectHashMapper) {

				return new HashMapper<V, HK, HV>() {

					@Override
					public Map<HK, HV> toHash(V object) {
						return (Map<HK, HV>) ((ObjectHashMapper) mapper).toObjectHash(object);
					}

					@Override
					public V fromHash(Map<HK, HV> hash) {

						Map<byte[], byte[]> map = hash.entrySet().stream()
								.collect(Collectors.toMap(e -> conversionService.convert((Object) e.getKey(), byte[].class),
										e -> conversionService.convert((Object) e.getValue(), byte[].class)));

						return (V) mapper.fromHash((Map<HK, HV>) map);
					}
				};

			}

			return (HashMapper<V, HK, HV>) mapper;
		}

		protected byte[] serializeHashKeyIfRequired(HK key) {

			return hashKeySerializerPresent() ? serialize(key, hashKeySerializer)
					: conversionService.convert(key, byte[].class);
		}

		protected boolean hashKeySerializerPresent() {
			return hashValueSerializer != null;
		}

		protected byte[] serializeHashValueIfRequires(HV value) {
			return hashValueSerializerPresent() ? serialize(value, hashValueSerializer)
					: conversionService.convert(value, byte[].class);
		}

		protected boolean hashValueSerializerPresent() {
			return hashValueSerializer != null;
		}

		protected byte[] serializeKeyIfRequired(K key) {
			return keySerializerPresent() ? serialize(key, keySerializer) : conversionService.convert(key, byte[].class);
		}

		protected boolean keySerializerPresent() {
			return keySerializer != null;
		}

		protected K deserializeKey(byte[] bytes, Class<K> targetType) {
			return keySerializerPresent() ? keySerializer.deserialize(bytes) : conversionService.convert(bytes, targetType);
		}

		protected HK deserializeHashKey(byte[] bytes, Class<HK> targetType) {

			return hashKeySerializerPresent() ? (HK) hashKeySerializer.deserialize(bytes)
					: conversionService.convert(bytes, targetType);
		}

		protected HV deserializeHashValue(byte[] bytes, Class<HV> targetType) {
			return hashValueSerializerPresent() ? (HV) hashValueSerializer.deserialize(bytes)
					: conversionService.convert(bytes, targetType);
		}

		byte[] serialize(Object value, RedisSerializer serializer) {

			Object _value = value;
			if (!serializer.canSerialize(value.getClass())) {
				_value = conversionService.convert(value, serializer.getTargetType());
			}
			return serializer.serialize(_value);
		}

		private Map.Entry<byte[], byte[]> mapToBinary(Map.Entry<? extends HK, ? extends HV> it) {

			return new Map.Entry<byte[], byte[]>() {

				@Override
				public byte[] getKey() {
					return serializeHashKeyIfRequired(it.getKey());
				}

				@Override
				public byte[] getValue() {
					return serializeHashValueIfRequires(it.getValue());
				}

				@Override
				public byte[] setValue(byte[] value) {
					return new byte[0];
				}
			};
		}

		private Map.Entry<HK, HV> mapToObject(Map.Entry<byte[], byte[]> pair) {

			return new Map.Entry<HK, HV>() {

				@Override
				public HK getKey() {
					return deserializeHashKey(pair.getKey(), (Class<HK>) Object.class);
				}

				@Override
				public HV getValue() {
					return deserializeHashValue(pair.getValue(), (Class<HV>) Object.class);
				}

				@Override
				public HV setValue(HV value) {
					return value;
				}

			};
		}
	}

}
