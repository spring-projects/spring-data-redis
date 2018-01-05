/*
 * Copyright 2011-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link ListOperations}.
 *
 * @author Costin Leau
 * @author David Liu
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
class DefaultListOperations<K, V> extends AbstractOperations<K, V> implements ListOperations<K, V> {

	DefaultListOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#index(java.lang.Object, long)
	 */
	@Override
	public V index(K key, long index) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.lIndex(rawKey, index);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPop(java.lang.Object)
	 */
	@Override
	public V leftPop(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.lPop(rawKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPop(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V leftPop(K key, long timeout, TimeUnit unit) {

		int tm = (int) TimeoutUtils.toSeconds(timeout, unit);
		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				List<byte[]> lPop = connection.bLPop(tm, rawKey);
				return (CollectionUtils.isEmpty(lPop) ? null : lPop.get(1));
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long leftPush(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.lPush(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPushAll(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long leftPushAll(K key, V... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);
		return execute(connection -> connection.lPush(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPushAll(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Long leftPushAll(K key, Collection<V> values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);

		return execute(connection -> connection.lPush(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPushIfPresent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long leftPushIfPresent(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.lPushX(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#leftPush(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long leftPush(K key, V pivot, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawPivot = rawValue(pivot);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.lInsert(rawKey, Position.BEFORE, rawPivot, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> connection.lLen(rawKey), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#range(java.lang.Object, long, long)
	 */
	@Override
	public List<V> range(K key, long start, long end) {

		byte[] rawKey = rawKey(key);
		return execute(connection -> deserializeValues(connection.lRange(rawKey, start, end)), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#remove(java.lang.Object, long, java.lang.Object)
	 */
	@Override
	public Long remove(K key, long count, Object value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.lRem(rawKey, count, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPop(java.lang.Object)
	 */
	@Override
	public V rightPop(K key) {

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.rPop(rawKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPop(java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V rightPop(K key, long timeout, TimeUnit unit) {

		int tm = (int) TimeoutUtils.toSeconds(timeout, unit);

		return execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				List<byte[]> bRPop = connection.bRPop(tm, rawKey);
				return (CollectionUtils.isEmpty(bRPop) ? null : bRPop.get(1));
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long rightPush(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.rPush(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPushAll(java.lang.Object, java.lang.Object[])
	 */
	@Override
	public Long rightPushAll(K key, V... values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);
		return execute(connection -> connection.rPush(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPushAll(java.lang.Object, java.util.Collection)
	 */
	@Override
	public Long rightPushAll(K key, Collection<V> values) {

		byte[] rawKey = rawKey(key);
		byte[][] rawValues = rawValues(values);
		return execute(connection -> connection.rPush(rawKey, rawValues), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPushIfPresent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long rightPushIfPresent(K key, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.rPushX(rawKey, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPush(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Long rightPush(K key, V pivot, V value) {

		byte[] rawKey = rawKey(key);
		byte[] rawPivot = rawValue(pivot);
		byte[] rawValue = rawValue(value);
		return execute(connection -> connection.lInsert(rawKey, Position.AFTER, rawPivot, rawValue), true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPopAndLeftPush(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V rightPopAndLeftPush(K sourceKey, K destinationKey) {

		byte[] rawDestKey = rawKey(destinationKey);
		return execute(new ValueDeserializingRedisCallback(sourceKey) {

			@Override
			protected byte[] inRedis(byte[] rawSourceKey, RedisConnection connection) {
				return connection.rPopLPush(rawSourceKey, rawDestKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#rightPopAndLeftPush(java.lang.Object, java.lang.Object, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public V rightPopAndLeftPush(K sourceKey, K destinationKey, long timeout, TimeUnit unit) {

		int tm = (int) TimeoutUtils.toSeconds(timeout, unit);
		byte[] rawDestKey = rawKey(destinationKey);
		return execute(new ValueDeserializingRedisCallback(sourceKey) {

			@Override
			protected byte[] inRedis(byte[] rawSourceKey, RedisConnection connection) {
				return connection.bRPopLPush(tm, rawSourceKey, rawDestKey);
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#set(java.lang.Object, long, java.lang.Object)
	 */
	@Override
	public void set(K key, long index, V value) {

		byte[] rawValue = rawValue(value);
		execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				connection.lSet(rawKey, index, rawValue);
				return null;
			}
		}, true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.ListOperations#trim(java.lang.Object, long, long)
	 */
	@Override
	public void trim(K key, long start, long end) {

		execute(new ValueDeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				connection.lTrim(rawKey, start, end);
				return null;
			}
		}, true);
	}
}
