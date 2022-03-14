/*
 * Copyright 2011-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Internal base class used by various RedisTemplate XXXOperations implementations.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author David Liu
 * @author Mark Paluch
 * @author Denis Zavedeev
 */
abstract class AbstractOperations<K, V> {

	// utility methods for the template internal methods
	abstract class ValueDeserializingRedisCallback implements RedisCallback<V> {
		private Object key;

		public ValueDeserializingRedisCallback(Object key) {
			this.key = key;
		}

		public final V doInRedis(RedisConnection connection) {
			byte[] result = inRedis(rawKey(key), connection);
			return deserializeValue(result);
		}

		@Nullable
		protected abstract byte[] inRedis(byte[] rawKey, RedisConnection connection);
	}

	final RedisTemplate<K, V> template;

	AbstractOperations(RedisTemplate<K, V> template) {
		this.template = template;
	}

	RedisSerializer keySerializer() {
		return template.getKeySerializer();
	}

	RedisSerializer valueSerializer() {
		return template.getValueSerializer();
	}

	RedisSerializer hashKeySerializer() {
		return template.getHashKeySerializer();
	}

	RedisSerializer hashValueSerializer() {
		return template.getHashValueSerializer();
	}

	RedisSerializer stringSerializer() {
		return template.getStringSerializer();
	}

	@Nullable
	<T> T execute(RedisCallback<T> callback) {
		return template.execute(callback, true);
	}

	public RedisOperations<K, V> getOperations() {
		return template;
	}

	@SuppressWarnings("unchecked")
	byte[] rawKey(Object key) {

		Assert.notNull(key, "non null key required");

		if (keySerializer() == null && key instanceof byte[]) {
			return (byte[]) key;
		}

		return keySerializer().serialize(key);
	}

	@SuppressWarnings("unchecked")
	byte[] rawString(String key) {
		return stringSerializer().serialize(key);
	}

	@SuppressWarnings("unchecked")
	byte[] rawValue(Object value) {

		if (valueSerializer() == null && value instanceof byte[]) {
			return (byte[]) value;
		}

		return valueSerializer().serialize(value);
	}

	byte[][] rawValues(Object... values) {

		byte[][] rawValues = new byte[values.length][];
		int i = 0;
		for (Object value : values) {
			rawValues[i++] = rawValue(value);
		}

		return rawValues;
	}

	/**
	 * @param values must not be {@literal empty} nor contain {@literal null} values.
	 * @return
	 * @since 1.5
	 */
	byte[][] rawValues(Collection<V> values) {

		Assert.notEmpty(values, "Values must not be 'null' or empty.");
		Assert.noNullElements(values.toArray(), "Values must not contain 'null' value.");

		byte[][] rawValues = new byte[values.size()][];
		int i = 0;
		for (V value : values) {
			rawValues[i++] = rawValue(value);
		}

		return rawValues;
	}

	@SuppressWarnings("unchecked")
	<HK> byte[] rawHashKey(HK hashKey) {
		Assert.notNull(hashKey, "non null hash key required");
		if (hashKeySerializer() == null && hashKey instanceof byte[]) {
			return (byte[]) hashKey;
		}
		return hashKeySerializer().serialize(hashKey);
	}

	<HK> byte[][] rawHashKeys(HK... hashKeys) {

		byte[][] rawHashKeys = new byte[hashKeys.length][];
		int i = 0;
		for (HK hashKey : hashKeys) {
			rawHashKeys[i++] = rawHashKey(hashKey);
		}
		return rawHashKeys;
	}

	@SuppressWarnings("unchecked")
	<HV> byte[] rawHashValue(HV value) {

		if (hashValueSerializer() == null && value instanceof byte[]) {
			return (byte[]) value;
		}
		return hashValueSerializer().serialize(value);
	}

	byte[][] rawKeys(K key, K otherKey) {

		byte[][] rawKeys = new byte[2][];

		rawKeys[0] = rawKey(key);
		rawKeys[1] = rawKey(key);
		return rawKeys;
	}

	byte[][] rawKeys(Collection<K> keys) {
		return rawKeys(null, keys);
	}

	byte[][] rawKeys(K key, Collection<K> keys) {

		byte[][] rawKeys = new byte[keys.size() + (key != null ? 1 : 0)][];

		int i = 0;

		if (key != null) {
			rawKeys[i++] = rawKey(key);
		}

		for (K k : keys) {
			rawKeys[i++] = rawKey(k);
		}

		return rawKeys;
	}

	@SuppressWarnings("unchecked")
	Set<V> deserializeValues(Set<byte[]> rawValues) {
		if (valueSerializer() == null) {
			return (Set<V>) rawValues;
		}
		return SerializationUtils.deserialize(rawValues, valueSerializer());
	}

	@Nullable
	Set<TypedTuple<V>> deserializeTupleValues(@Nullable Set<Tuple> rawValues) {
		if (rawValues == null) {
			return null;
		}
		Set<TypedTuple<V>> set = new LinkedHashSet<>(rawValues.size());
		for (Tuple rawValue : rawValues) {
			set.add(deserializeTuple(rawValue));
		}
		return set;
	}

	List<TypedTuple<V>> deserializeTupleValues(List<Tuple> rawValues) {
		if (rawValues == null) {
			return null;
		}
		List<TypedTuple<V>> set = new ArrayList<>(rawValues.size());
		for (Tuple rawValue : rawValues) {
			set.add(deserializeTuple(rawValue));
		}
		return set;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Nullable
	TypedTuple<V> deserializeTuple(@Nullable Tuple tuple) {
		if (tuple == null) {
			return null;
		}
		Object value = tuple.getValue();
		if (valueSerializer() != null) {
			value = valueSerializer().deserialize(tuple.getValue());
		}
		return new DefaultTypedTuple(value, tuple.getScore());
	}

	@SuppressWarnings("unchecked")
	Set<Tuple> rawTupleValues(Set<TypedTuple<V>> values) {
		if (values == null) {
			return null;
		}
		Set<Tuple> rawTuples = new LinkedHashSet<>(values.size());
		for (TypedTuple<V> value : values) {
			byte[] rawValue;
			if (valueSerializer() == null && value.getValue() instanceof byte[]) {
				rawValue = (byte[]) value.getValue();
			} else {
				rawValue = valueSerializer().serialize(value.getValue());
			}
			rawTuples.add(new DefaultTuple(rawValue, value.getScore()));
		}
		return rawTuples;
	}

	@SuppressWarnings("unchecked")
	List<V> deserializeValues(List<byte[]> rawValues) {
		if (valueSerializer() == null) {
			return (List<V>) rawValues;
		}
		return SerializationUtils.deserialize(rawValues, valueSerializer());
	}

	@SuppressWarnings("unchecked")
	<T> Set<T> deserializeHashKeys(Set<byte[]> rawKeys) {
		if (hashKeySerializer() == null) {
			return (Set<T>) rawKeys;
		}
		return SerializationUtils.deserialize(rawKeys, hashKeySerializer());
	}

	@SuppressWarnings("unchecked")
	<T> List<T> deserializeHashKeys(List<byte[]> rawKeys) {
		if (hashKeySerializer() == null) {
			return (List<T>) rawKeys;
		}
		return SerializationUtils.deserialize(rawKeys, hashKeySerializer());
	}

	@SuppressWarnings("unchecked")
	<T> List<T> deserializeHashValues(List<byte[]> rawValues) {
		if (hashValueSerializer() == null) {
			return (List<T>) rawValues;
		}
		return SerializationUtils.deserialize(rawValues, hashValueSerializer());
	}

	@SuppressWarnings("unchecked")
	<HK, HV> Map<HK, HV> deserializeHashMap(@Nullable Map<byte[], byte[]> entries) {
		// connection in pipeline/multi mode

		if (entries == null) {
			return null;
		}

		Map<HK, HV> map = new LinkedHashMap<>(entries.size());

		for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
			map.put((HK) deserializeHashKey(entry.getKey()), (HV) deserializeHashValue(entry.getValue()));
		}

		return map;
	}

	@SuppressWarnings("unchecked")
	K deserializeKey(byte[] value) {
		if (keySerializer() == null) {
			return (K) value;
		}
		return (K) keySerializer().deserialize(value);
	}

	/**
	 * @param keys
	 * @return
	 * @since 1.7
	 */
	Set<K> deserializeKeys(Set<byte[]> keys) {

		if (CollectionUtils.isEmpty(keys)) {
			return Collections.emptySet();
		}
		Set<K> result = new LinkedHashSet<>(keys.size());
		for (byte[] key : keys) {
			result.add(deserializeKey(key));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	V deserializeValue(byte[] value) {
		if (valueSerializer() == null) {
			return (V) value;
		}
		return (V) valueSerializer().deserialize(value);
	}

	String deserializeString(byte[] value) {
		return (String) stringSerializer().deserialize(value);
	}

	@SuppressWarnings({ "unchecked" })
	<HK> HK deserializeHashKey(byte[] value) {
		if (hashKeySerializer() == null) {
			return (HK) value;
		}
		return (HK) hashKeySerializer().deserialize(value);
	}

	@SuppressWarnings("unchecked")
	<HV> HV deserializeHashValue(byte[] value) {
		if (hashValueSerializer() == null) {
			return (HV) value;
		}
		return (HV) hashValueSerializer().deserialize(value);
	}

	/**
	 * Deserialize {@link GeoLocation} of {@link GeoResults}.
	 *
	 * @param source can be {@literal null}.
	 * @return converted or {@literal null}.
	 * @since 1.8
	 */
	GeoResults<GeoLocation<V>> deserializeGeoResults(GeoResults<GeoLocation<byte[]>> source) {

		if (source == null) {
			return null;
		}

		if (valueSerializer() == null) {
			return (GeoResults<GeoLocation<V>>) (Object) source;
		}

		return Converters.deserializingGeoResultsConverter((RedisSerializer<V>) valueSerializer()).convert(source);
	}
}
