/*
 * Copyright 2011-present the original author or authors.
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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.DefaultedRedisConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands.SetCondition;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;

/**
 * Default implementation of {@link ValueOperations}.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Jiahe Cai
 * @author Ehsan Alemzadeh
 * @author Chris Bono
 */
class DefaultValueOperations<K, V> extends AbstractOperations<K, V> implements ValueOperations<K, V> {

	DefaultValueOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	@Override
	public @Nullable V get(Object key) {
		return execute(valueCallbackFor(key, DefaultedRedisConnection::get));
	}

	@Override
	public @Nullable V getAndDelete(K key) {
		return execute(valueCallbackFor(key, DefaultedRedisConnection::getDel));
	}

	@Override
	public @Nullable V getAndExpire(K key, long timeout, TimeUnit unit) {
		return execute(
				valueCallbackFor(key, (connection, rawKey) -> connection.getEx(rawKey, Expiration.from(timeout, unit))));
	}

	@Override
	public @Nullable V getAndExpire(K key, Duration timeout) {
		return execute(valueCallbackFor(key, (connection, rawKey) -> connection.getEx(rawKey, Expiration.from(timeout))));
	}

	@Override
	public @Nullable V getAndPersist(K key) {
		return execute(valueCallbackFor(key, (connection, rawKey) -> connection.getEx(rawKey, Expiration.persistent())));
	}

	@Override
	public @Nullable V getAndSet(K key, V newValue) {

		byte[] rawValue = rawValue(newValue);
		return execute(valueCallbackFor(key, (connection, rawKey) -> connection.getSet(rawKey, rawValue)));
	}

	@Override
	public Long increment(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.incr(rawKey));
	}

	@Override
	public Long increment(K key, long delta) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.incrBy(rawKey, delta));
	}

	@Override
	public Double increment(K key, double delta) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.incrBy(rawKey, delta));
	}

	@Override
	public Long decrement(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.decr(rawKey));
	}

	@Override
	public Long decrement(K key, long delta) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.decrBy(rawKey, delta));
	}

	@Override
	@SuppressWarnings("NullAway")
	public Integer append(K key, String value) {

		byte[] rawKey = rawKey(key);
		byte[] rawString = rawString(value);

		return execute(connection -> {
			Long result = connection.append(rawKey, rawString);
			return (result != null) ? result.intValue() : null;
		});
	}

	@Override
	public String get(K key, long start, long end) {
		byte[] rawKey = rawKey(key);
		byte[] rawReturn = execute(connection -> connection.getRange(rawKey, start, end));

		return deserializeString(rawReturn);
	}

	@Override
	public List<@Nullable V> multiGet(Collection<K> keys) {

		if (keys.isEmpty()) {
			return Collections.emptyList();
		}

		byte[][] rawKeys = new byte[keys.size()][];

		int counter = 0;
		for (K hashKey : keys) {
			rawKeys[counter++] = rawKey(hashKey);
		}

		List<byte[]> rawValues = execute(connection -> connection.mGet(rawKeys));

		return deserializeValues(rawValues);
	}

	@Override
	@SuppressWarnings("NullAway")
	public void multiSet(Map<? extends K, ? extends V> m) {

		if (m.isEmpty()) {
			return;
		}

		Map<byte[], byte[]> rawKeys = new LinkedHashMap<>(m.size());

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		execute((RedisCallback<@Nullable Void>) connection -> {
			connection.mSet(rawKeys);
			return null;
		});
	}

	@Override
	public Boolean multiSetIfAbsent(Map<? extends K, ? extends V> m) {

		if (m.isEmpty()) {
			return true;
		}

		Map<byte[], byte[]> rawKeys = new LinkedHashMap<>(m.size());

		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
		}

		return execute(connection -> connection.mSetNX(rawKeys));
	}

	@Override
	public void set(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		execute(connection -> connection.set(rawKey, rawValue));
	}

	@Override
	public void set(K key, V value, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		execute(connection -> connection.set(rawKey, rawValue, Expiration.from(timeout, unit), SetOption.upsert()));
	}

	@Override
	public @Nullable V setGet(K key, V value, long timeout, TimeUnit unit) {
		return doSetGet(key, value, Expiration.from(timeout, unit));
	}

	@Override
	public @Nullable V setGet(K key, V value, Duration duration) {
		return doSetGet(key, value, Expiration.from(duration));
	}

	private @Nullable V doSetGet(K key, V value, Expiration duration) {

		byte[] rawValue = rawValue(value);
		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.stringCommands().setGet(rawKey, rawValue, duration, SetOption.UPSERT);
			}
		});
	}

	@Override
	public Boolean setIfAbsent(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.set(rawKey, rawValue, Expiration.persistent(), SetOption.ifAbsent()));
	}

	@Override
	public Boolean setIfAbsent(K key, V value, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		Expiration expiration = Expiration.from(timeout, unit);
		return execute(connection -> connection.set(rawKey, rawValue, expiration, SetOption.ifAbsent()));
	}

	@Nullable
	@Override
	public Boolean setIfPresent(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		return execute(connection -> connection.set(rawKey, rawValue, Expiration.persistent(), SetOption.ifPresent()));
	}

	@Nullable
	@Override
	public Boolean setIfPresent(K key, V value, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		Expiration expiration = Expiration.from(timeout, unit);
		return execute(connection -> connection.set(rawKey, rawValue, expiration, SetOption.ifPresent()));
	}

	@Override
	public Boolean setIfEqual(K key, V newValue, V oldValue) {

		byte[] rawKey = rawKey(key);
		byte[] rawNewValue = rawValue(newValue);
		byte[] rawOldValue = rawValue(oldValue);

		return execute(connection -> connection.set(rawKey, rawNewValue, Expiration.persistent(), SetCondition.ifValueEqual(rawOldValue)));
	}

	@Override
	public Boolean setIfEqual(K key, V newValue, V oldValue, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawNewValue = rawValue(newValue);
		byte[] rawOldValue = rawValue(oldValue);

		Expiration expiration = Expiration.from(timeout, unit);
		return execute(connection -> connection.set(rawKey, rawNewValue, expiration, SetCondition.ifValueEqual(rawOldValue)));
	}

	@Override
	public Boolean setIfNotEqual(K key, V newValue, V oldValue) {

		byte[] rawKey = rawKey(key);
		byte[] rawNewValue = rawValue(newValue);
		byte[] rawOldValue = rawValue(oldValue);

		return execute(connection -> connection.set(rawKey, rawNewValue, Expiration.persistent(), SetCondition.ifValueNotEqual(rawOldValue)));
	}

	@Override
	public Boolean setIfNotEqual(K key, V newValue, V oldValue, long timeout, TimeUnit unit) {

		byte[] rawKey = rawKey(key);
		byte[] rawNewValue = rawValue(newValue);
		byte[] rawOldValue = rawValue(oldValue);

		Expiration expiration = Expiration.from(timeout, unit);
		return execute(connection -> connection.set(rawKey, rawNewValue, expiration, SetCondition.ifValueNotEqual(rawOldValue)));
	}

	@Override
	@SuppressWarnings("NullAway")
	public void set(K key, V value, long offset) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);

		execute(connection -> {
			connection.setRange(rawKey, rawValue, offset);
			return null;
		});
	}

	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.strLen(rawKey));
	}

	@Override
	public Boolean setBit(K key, long offset, boolean value) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.setBit(rawKey, offset, value));
	}

	@Override
	public Boolean getBit(K key, long offset) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.getBit(rawKey, offset));
	}

	@Override
	public List<Long> bitField(K key, final BitFieldSubCommands subCommands) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.bitField(rawKey, subCommands));
	}
}
