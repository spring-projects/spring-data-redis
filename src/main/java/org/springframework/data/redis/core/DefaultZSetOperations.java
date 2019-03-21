/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;

/**
 * Default implementation of {@link ZSetOperations}.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author David Liu
 * @author Mark Paluch
 */
public class DefaultZSetOperations<K, V> extends AbstractOperations<K, V> implements ZSetOperations<K, V> {

	/**
	 * Constructs a new {@link DefaultZSetOperations} instance.
	 * 
	 * @param template must not be {@literal null}.
	 */
	public DefaultZSetOperations(RedisTemplate<K, V> template) {
		super(template);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#add(java.lang.Object, java.lang.Object, double)
	 */
	public Boolean add(final K key, final V value, final double score) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(value);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				return connection.zAdd(rawKey, score, rawValue);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#add(java.lang.Object, java.util.Set)
	 */
	public Long add(K key, Set<TypedTuple<V>> tuples) {
		final byte[] rawKey = rawKey(key);
		final Set<Tuple> rawValues = rawTupleValues(tuples);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zAdd(rawKey, rawValues);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#incrementScore(java.lang.Object, java.lang.Object, double)
	 */
	public Double incrementScore(K key, V value, final double delta) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(value);

		return execute(new RedisCallback<Double>() {

			public Double doInRedis(RedisConnection connection) {
				return connection.zIncrBy(rawKey, delta, rawValue);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#intersectAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	public Long intersectAndStore(K key, K otherKey, K destKey) {
		return intersectAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#intersectAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	public Long intersectAndStore(K key, Collection<K> otherKeys, K destKey) {
		final byte[][] rawKeys = rawKeys(key, otherKeys);
		final byte[] rawDestKey = rawKey(destKey);
		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zInterStore(rawDestKey, rawKeys);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#range(java.lang.Object, long, long)
	 */
	public Set<V> range(K key, final long start, final long end) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRange(rawKey, start, end);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRange(java.lang.Object, long, long)
	 */
	public Set<V> reverseRange(K key, final long start, final long end) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRevRange(rawKey, start, end);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeWithScores(java.lang.Object, long, long)
	 */
	public Set<TypedTuple<V>> rangeWithScores(K key, final long start, final long end) {
		final byte[] rawKey = rawKey(key);

		Set<Tuple> rawValues = execute(new RedisCallback<Set<Tuple>>() {

			public Set<Tuple> doInRedis(RedisConnection connection) {
				return connection.zRangeWithScores(rawKey, start, end);
			}
		}, true);

		return deserializeTupleValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeWithScores(java.lang.Object, long, long)
	 */
	public Set<TypedTuple<V>> reverseRangeWithScores(K key, final long start, final long end) {
		final byte[] rawKey = rawKey(key);

		Set<Tuple> rawValues = execute(new RedisCallback<Set<Tuple>>() {

			public Set<Tuple> doInRedis(RedisConnection connection) {
				return connection.zRevRangeWithScores(rawKey, start, end);
			}
		}, true);

		return deserializeTupleValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByLex(java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Range)
	 */
	@Override
	public Set<V> rangeByLex(K key, final Range range) {
		return rangeByLex(key, range, null);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByLex(java.lang.Object, org.springframework.data.redis.connection.RedisZSetCommands.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Override
	public Set<V> rangeByLex(K key, final Range range, final Limit limit) {

		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRangeByLex(rawKey, range, limit);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScore(java.lang.Object, double, double)
	 */
	public Set<V> rangeByScore(K key, final double min, final double max) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRangeByScore(rawKey, min, max);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScore(java.lang.Object, double, double, long, long)
	 */
	public Set<V> rangeByScore(K key, final double min, final double max, final long offset, final long count) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRangeByScore(rawKey, min, max, offset, count);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScore(java.lang.Object, double, double)
	 */
	public Set<V> reverseRangeByScore(K key, final double min, final double max) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRevRangeByScore(rawKey, min, max);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScore(java.lang.Object, double, double, long, long)
	 */
	public Set<V> reverseRangeByScore(K key, final double min, final double max, final long offset, final long count) {
		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRevRangeByScore(rawKey, min, max, offset, count);
			}
		}, true);

		return deserializeValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScoreWithScores(java.lang.Object, double, double)
	 */
	public Set<TypedTuple<V>> rangeByScoreWithScores(K key, final double min, final double max) {
		final byte[] rawKey = rawKey(key);

		Set<Tuple> rawValues = execute(new RedisCallback<Set<Tuple>>() {

			public Set<Tuple> doInRedis(RedisConnection connection) {
				return connection.zRangeByScoreWithScores(rawKey, min, max);
			}
		}, true);

		return deserializeTupleValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rangeByScoreWithScores(java.lang.Object, double, double, long, long)
	 */
	public Set<TypedTuple<V>> rangeByScoreWithScores(K key, final double min, final double max, final long offset,
			final long count) {
		final byte[] rawKey = rawKey(key);

		Set<Tuple> rawValues = execute(new RedisCallback<Set<Tuple>>() {

			public Set<Tuple> doInRedis(RedisConnection connection) {
				return connection.zRangeByScoreWithScores(rawKey, min, max, offset, count);
			}
		}, true);

		return deserializeTupleValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScoreWithScores(java.lang.Object, double, double)
	 */
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, final double min, final double max) {
		final byte[] rawKey = rawKey(key);

		Set<Tuple> rawValues = execute(new RedisCallback<Set<Tuple>>() {

			public Set<Tuple> doInRedis(RedisConnection connection) {
				return connection.zRevRangeByScoreWithScores(rawKey, min, max);

			}
		}, true);

		return deserializeTupleValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRangeByScoreWithScores(java.lang.Object, double, double, long, long)
	 */
	public Set<TypedTuple<V>> reverseRangeByScoreWithScores(K key, final double min, final double max, final long offset,
			final long count) {
		final byte[] rawKey = rawKey(key);

		Set<Tuple> rawValues = execute(new RedisCallback<Set<Tuple>>() {

			public Set<Tuple> doInRedis(RedisConnection connection) {
				return connection.zRevRangeByScoreWithScores(rawKey, min, max, offset, count);

			}
		}, true);

		return deserializeTupleValues(rawValues);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#rank(java.lang.Object, java.lang.Object)
	 */
	public Long rank(K key, Object o) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(o);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				Long zRank = connection.zRank(rawKey, rawValue);
				return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#reverseRank(java.lang.Object, java.lang.Object)
	 */
	public Long reverseRank(K key, Object o) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(o);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				Long zRank = connection.zRevRank(rawKey, rawValue);
				return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#remove(java.lang.Object, java.lang.Object[])
	 */
	public Long remove(K key, Object... values) {
		final byte[] rawKey = rawKey(key);
		final byte[][] rawValues = rawValues(values);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zRem(rawKey, rawValues);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#removeRange(java.lang.Object, long, long)
	 */
	public Long removeRange(K key, final long start, final long end) {
		final byte[] rawKey = rawKey(key);
		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zRemRange(rawKey, start, end);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#removeRangeByScore(java.lang.Object, double, double)
	 */
	public Long removeRangeByScore(K key, final double min, final double max) {
		final byte[] rawKey = rawKey(key);
		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zRemRangeByScore(rawKey, min, max);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#score(java.lang.Object, java.lang.Object)
	 */
	public Double score(K key, Object o) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawValue = rawValue(o);

		return execute(new RedisCallback<Double>() {

			public Double doInRedis(RedisConnection connection) {
				return connection.zScore(rawKey, rawValue);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#count(java.lang.Object, double, double)
	 */
	public Long count(K key, final double min, final double max) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zCount(rawKey, min, max);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#size(java.lang.Object)
	 */
	@Override
	public Long size(K key) {
		return zCard(key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#zCard(java.lang.Object)
	 */
	@Override
	public Long zCard(K key) {

		final byte[] rawKey = rawKey(key);
		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zCard(rawKey);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#unionAndStore(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	public Long unionAndStore(K key, K otherKey, K destKey) {
		return unionAndStore(key, Collections.singleton(otherKey), destKey);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#unionAndStore(java.lang.Object, java.util.Collection, java.lang.Object)
	 */
	public Long unionAndStore(K key, Collection<K> otherKeys, K destKey) {
		final byte[][] rawKeys = rawKeys(key, otherKeys);
		final byte[] rawDestKey = rawKey(destKey);
		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.zUnionStore(rawDestKey, rawKeys);
			}
		}, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ZSetOperations#scan(java.lang.Object, org.springframework.data.redis.core.ScanOptions)
	 */
	@Override
	public Cursor<TypedTuple<V>> scan(K key, final ScanOptions options) {

		final byte[] rawKey = rawKey(key);
		Cursor<Tuple> cursor = template.executeWithStickyConnection(new RedisCallback<Cursor<Tuple>>() {

			@Override
			public Cursor<Tuple> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.zScan(rawKey, options);
			}
		});

		return new ConvertingCursor<Tuple, TypedTuple<V>>(cursor, new Converter<Tuple, TypedTuple<V>>() {

			@Override
			public TypedTuple<V> convert(Tuple source) {
				return deserializeTuple(source);
			}
		});
	}

	/**
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public Set<byte[]> rangeByScore(K key, final String min, final String max) {

		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRangeByScore(rawKey, min, max);
			}
		}, true);

		return rawValues;
	}

	/**
	 * @param key
	 * @param min
	 * @param max
	 * @param offset
	 * @param count
	 * @return
	 */
	public Set<byte[]> rangeByScore(K key, final String min, final String max, final long offset, final long count) {

		final byte[] rawKey = rawKey(key);

		Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.zRangeByScore(rawKey, min, max, offset, count);
			}
		}, true);

		return rawValues;
	}
}
