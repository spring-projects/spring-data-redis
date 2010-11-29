/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.SimpleRedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.StringRedisSerializer;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * 
 * Helper class that simplifies Redis data access code. Automatically converts Redis connection exceptions into 
 * DataAccessExceptions, following the org.springframework.dao exception hierarchy.
 *
 * The central method is execute, supporting Redis access code implementing the {@link RedisCallback} interface.
 * It provides {@link RedisConnection} handling such that neither the {@link RedisCallback} implementation nor 
 * the calling code needs to explicitly care about retrieving/closing Redis connections, or handling Session 
 * lifecycle exceptions. For typical single step actions, there are various convenience methods.
 *  
 * <b>This is the central class in Redis support</b>.
 * Simplifies the use of Redis and helps avoid common errors.
 * 
 * @author Costin Leau
 */
public class RedisTemplate<K, V> extends RedisAccessor implements RedisOperations<K, V> {

	private boolean exposeConnection = false;
	private RedisSerializer keySerializer = new StringRedisSerializer();
	private RedisSerializer valueSerializer = new SimpleRedisSerializer();
	private RedisSerializer hashKeySerializer = new SimpleRedisSerializer();
	private RedisSerializer hashValueSerializer = new SimpleRedisSerializer();

	public RedisTemplate() {
	}

	public RedisTemplate(RedisConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	public void del(final String redisKey) {
		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.del(keySerializer.serialize(redisKey));
				return null;
			}
		});
	}


	public <T> T execute(RedisCallback<T> action) {
		return execute(action, isExposeConnection());
	}

	public <T> T execute(RedisCallback<T> action, boolean exposeConnection) {
		return execute(action, exposeConnection, valueSerializer);
	}

	public <T> T execute(RedisCallback<T> action, boolean exposeConnection, RedisSerializer returnSerializer) {
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		RedisConnection conn = RedisConnectionUtils.getRedisConnection(factory);

		boolean existingConnection = TransactionSynchronizationManager.hasResource(factory);

		try {
			RedisConnection connToExpose = (exposeConnection ? conn : createRedisConnectionProxy(conn));
			T result = action.doInRedis(connToExpose);
			// TODO: should do flush?
			return postProcessResult(result, conn, existingConnection);
		} finally {
			RedisConnectionUtils.releaseConnection(conn, factory);
		}
	}

	protected RedisConnection createRedisConnectionProxy(RedisConnection pm) {
		Class<?>[] ifcs = ClassUtils.getAllInterfacesForClass(pm.getClass(), getClass().getClassLoader());
		return (RedisConnection) Proxy.newProxyInstance(pm.getClass().getClassLoader(), ifcs,
				new CloseSuppressingInvocationHandler(pm));
	}

	protected <T> T postProcessResult(T result, RedisConnection conn, boolean existingConnection) {
		return result;
	}

	/**
	 * Returns the exposeConnection.
	 *
	 * @return Returns the exposeConnection
	 */
	public boolean isExposeConnection() {
		return exposeConnection;
	}

	/**
	 * Sets whether to expose the Redis connection to {@link RedisCallback} code.
	 * 
	 * Default is "false": a proxy will be returned, suppressing <tt>quit</tt> and <tt>disconnect</tt> calls.
	 *  
	 * @param exposeConnection
	 */
	public void setExposeConnection(boolean exposeConnection) {
		this.exposeConnection = exposeConnection;
	}

	/**
	 * Sets the key serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}.
	 * 
	 * @param serializer
	 */
	public void setKeySerializer(RedisSerializer serializer) {
		this.keySerializer = serializer;
	}

	/**
	 * Sets the value serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}.
	 * 
	 * @param serializer
	 */
	public void setValueSerializer(RedisSerializer serializer) {
		this.valueSerializer = serializer;
	}

	/**
	 * Sets the hash key (or field) serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}. 
	 * 
	 * @param hashKeySerializer The hashKeySerializer to set.
	 */
	public void setHashKeySerializer(RedisSerializer hashKeySerializer) {
		this.hashKeySerializer = hashKeySerializer;
	}

	/**
	 * Sets the hash value serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}. 
	 * 
	 * @param hashValueSerializer The hashValueSerializer to set.
	 */
	public void setHashValueSerializer(RedisSerializer hashValueSerializer) {
		this.hashValueSerializer = hashValueSerializer;
	}


	/**
	 * Invocation handler that suppresses close calls on JDO PersistenceManagers.
	 * Also prepares returned Query objects.
	 * @see RedisConnection#close()
	 */
	private class CloseSuppressingInvocationHandler implements InvocationHandler {

		private final RedisConnection target;

		public CloseSuppressingInvocationHandler(RedisConnection target) {
			this.target = target;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			// Invocation on PersistenceManager interface (or provider-specific extension) coming in...

			if (method.getName().equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			}
			else if (method.getName().equals("hashCode")) {
				// Use hashCode of PersistenceManager proxy.
				return System.identityHashCode(proxy);
			}
			else if (method.getName().equals("close")) {
				// Handle close method: suppress, not valid.
				return null;
			}

			// Invoke method on target RedisConnection.
			try {
				Object retVal = method.invoke(this.target, args);
				return retVal;
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

	private byte[] rawKey(K key) {
		return (key != null ? keySerializer.serialize(key) : null);
	}

	private <T> byte[] rawValue(T value) {
		return (value != null ? valueSerializer.serialize(value) : null);
	}

	private byte[][] rawKeys(K... keys) {
		final byte[][] rawKeys = new byte[keys.length][];

		for (int i = 0; i < keys.length; i++) {
			rawKeys[i] = rawKey(keys[i]);
		}

		return rawKeys;
	}

	private <HK> byte[] rawHashKey(HK value) {
		return (value != null ? hashKeySerializer.serialize(value) : null);
	}

	private <HV> byte[] rawHashValue(HV value) {
		return (value != null ? hashValueSerializer.serialize(value) : null);
	}


	@SuppressWarnings("unchecked")
	private <T extends Collection<V>> T values(Collection<byte[]> rawValues, Class<? extends Collection> type) {
		Collection<V> values = (List.class.isAssignableFrom(type) ? new ArrayList<V>(rawValues.size())
				: new LinkedHashSet<V>(rawValues.size()));
		for (byte[] bs : rawValues) {
			if (bs != null) {
				values.add((V) valueSerializer.deserialize(bs));
			}
		}

		return (T) values;
	}

	@SuppressWarnings("unchecked")
	private <H> Collection<H> hashValues(Collection<byte[]> rawValues, Class<? extends Collection> type) {
		Collection<H> values = (List.class.isAssignableFrom(type) ? new ArrayList<H>(rawValues.size())
				: new LinkedHashSet<H>(rawValues.size()));
		for (byte[] bs : rawValues) {
			if (bs != null) {
				values.add((H) hashValueSerializer.deserialize(bs));
			}
		}

		return values;
	}

	@SuppressWarnings("unchecked")
	private K deserializeKey(byte[] value) {
		return (K) deserialize(value, keySerializer);
	}

	@SuppressWarnings("unchecked")
	private V deserializeValue(byte[] value) {
		return (V) deserialize(value, valueSerializer);
	}

	@SuppressWarnings("unchecked")
	private <HK> HK deserializeHashKey(byte[] value) {
		return (HK) deserialize(value, hashKeySerializer);
	}

	@SuppressWarnings("unchecked")
	private <HV> HV deserializeHashValue(byte[] value) {
		return (HV) deserialize(value, hashValueSerializer);
	}

	private <T> T deserialize(byte[] value, RedisSerializer<T> serializer) {
		if (isEmpty(value)) {
			return null;
		}
		return (T) serializer.deserialize(value);
	}

	private static boolean isEmpty(byte[] data) {
		return (data == null || data.length == 0);
	}

	// utility methods for the template internal methods
	private abstract class ValueDeserializingRedisCallback implements RedisCallback<V> {
		private K key;

		public ValueDeserializingRedisCallback(K key) {
			this.key = key;
		}

		@SuppressWarnings("unchecked")
		@Override
		public final V doInRedis(RedisConnection connection) {
			byte[] result = inRedis(rawKey(key), connection);
			return deserializeValue(result);
		}

		protected abstract byte[] inRedis(byte[] rawKey, RedisConnection connection);
	}


	//
	// RedisOperations
	//

	@Override
	public Object exec() {
		throw new UnsupportedOperationException();
	}

	@Override
	public BoundListOperations<K, V> forList(K key) {
		return new DefaultBoundListOperations<K, V>(key, this);
	}

	@Override
	public V get(final K key) {
		return execute(new ValueDeserializingRedisCallback(key) {
			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.get(rawKey);
			}
		}, true);
	}

	@Override
	public V getAndSet(K key, V newValue) {
		final byte[] rawValue = rawValue(newValue);
		return execute(new ValueDeserializingRedisCallback(key) {
			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getSet(rawKey, rawValue);
			}
		}, true);
	}

	@Override
	public Integer increment(K key, final int delta) {
		final byte[] rawKey = rawKey(key);
		return execute(new RedisCallback<Integer>() {
			@Override
			public Integer doInRedis(RedisConnection connection) {
				if (delta == 1) {
					return connection.incr(rawKey);
				}

				if (delta == -1) {
					return connection.decr(rawKey);
				}

				if (delta < 0) {
					return connection.decrBy(rawKey, delta);
				}

				return connection.incrBy(rawKey, delta);
			}
		}, true);
	}

	@Override
	public ListOperations<K, V> listOps() {
		return new DefaultListOperations();
	}

	@Override
	public void multi() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void set(K key, V value) {
		final byte[] rawValue = rawValue(value);
		execute(new ValueDeserializingRedisCallback(key) {
			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				connection.set(rawKey, rawValue);
				return null;
			}
		}, true);
	}

	@Override
	public void watch(K... keys) {
		final byte[][] rawKeys = rawKeys(keys);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.watch(rawKeys);
				return null;
			}
		}, true);
	}

	@Override
	public void delete(K... keys) {
		final byte[][] rawKeys = rawKeys(keys);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.del(rawKeys);
				return null;
			}
		}, true);
	}

	//
	// List operations
	//

	private class DefaultListOperations implements ListOperations<K, V> {

		@Override
		public List<V> blockingLeftPop(final int timeout, K... keys) {
			final byte[][] rawKeys = rawKeys(keys);

			return execute(new RedisCallback<List<V>>() {
				@Override
				public List<V> doInRedis(RedisConnection connection) {
					return values(connection.bLPop(timeout, rawKeys), List.class);
				}
			}, true);
		}

		@Override
		public List<V> blockingRightPop(final int timeout, K... keys) {
			final byte[][] rawKeys = rawKeys(keys);
			return execute(new RedisCallback<List<V>>() {
				@Override
				public List<V> doInRedis(RedisConnection connection) {
					return values(connection.bRPop(timeout, rawKeys), List.class);
				}
			}, true);
		}

		@Override
		public V index(K key, final int index) {
			return execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					return connection.lIndex(rawKey, index);
				}
			}, true);
		}

		@Override
		public V leftPop(K key) {
			return execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					return connection.lPop(rawKey);
				}
			}, true);
		}

		@Override
		public Integer leftPush(K key, V value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.lPush(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public Integer length(K key) {
			final byte[] rawKey = rawKey(key);
			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.lLen(rawKey);
				}
			}, true);
		}

		@Override
		public List<V> range(K key, final int start, final int end) {
			final byte[] rawKey = rawKey(key);
			return execute(new RedisCallback<List<V>>() {
				@Override
				public List<V> doInRedis(RedisConnection connection) {
					return values(connection.lRange(rawKey, start, end), List.class);
				}
			}, true);
		}

		@Override
		public Integer remove(K key, final int count, Object value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.lRem(rawKey, count, rawValue);
				}
			}, true);
		}

		@Override
		public V rightPop(K key) {
			return execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					return connection.rPop(rawKey);
				}
			}, true);
		}

		@Override
		public Integer rightPush(K key, V value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.rPush(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public void set(K key, final int index, V value) {
			final byte[] rawValue = rawValue(value);
			execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					connection.lSet(rawKey, index, rawValue);
					return null;
				}
			}, true);
		}

		@Override
		public void trim(K key, final int start, final int end) {
			execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					connection.lTrim(rawKey, start, end);
					return null;
				}
			}, true);
		}

		@Override
		public RedisOperations<K, V> getOperations() {
			return RedisTemplate.this;
		}
	}

	//
	// Set operations
	//

	private K[] aggregateKeys(K key, K... keys) {
		Object[] aggregate = new Object[keys.length + 1];
		aggregate[0] = key;
		for (int i = 0; i < keys.length; i++) {
			aggregate[i + 1] = keys[i];
		}

		return (K[]) aggregate;
	}

	@Override
	public BoundSetOperations<K, V> forSet(K key) {
		return new DefaultBoundSetOperations<K, V>(key, this);
	}

	@Override
	public SetOperations<K, V> setOps() {
		return new DefaultSetOperations();
	}

	private class DefaultSetOperations implements SetOperations<K, V> {

		@Override
		public Boolean add(K key, V value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) {
					return connection.sAdd(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public Set<V> diff(final K key, final K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sDiff(rawKeys);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public void diffAndStore(final K key, K destKey, final K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			final byte[] rawDestKey = rawKey(destKey);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.sDiffStore(rawDestKey, rawKeys);
					return null;
				}
			}, true);
		}

		@Override
		public RedisOperations<K, V> getOperations() {
			return RedisTemplate.this;
		}

		@Override
		public Set<V> intersect(K key, K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sInter(rawKeys);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public void intersectAndStore(K key, K destKey, K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			final byte[] rawDestKey = rawKey(destKey);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.sInterStore(rawDestKey, rawKeys);
					return null;
				}
			}, true);
		}

		@Override
		public boolean isMember(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);
			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) {
					return connection.sIsMember(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public Set<V> members(K key) {
			final byte[] rawKey = rawKey(key);
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sMembers(rawKey);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public boolean remove(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);
			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) {
					return connection.sRem(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public int size(K key) {
			final byte[] rawKey = rawKey(key);
			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.sCard(rawKey);
				}
			}, true);
		}

		@Override
		public Set<V> union(K key, K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sUnion(rawKeys);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public void unionAndStore(K key, K destKey, K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			final byte[] rawDestKey = rawKey(destKey);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.sUnionStore(rawDestKey, rawKeys);
					return null;
				}
			}, true);
		}
	}

	//
	// ZSet operations
	//

	@Override
	public BoundZSetOperations<K, V> forZSet(K key) {
		return new DefaultBoundZSetOperations<K, V>(key, this);
	}

	@Override
	public ZSetOperations<K, V> zSetOps() {
		return new DefaultZSetOperations();
	}

	private class DefaultZSetOperations implements ZSetOperations<K, V> {

		@Override
		public boolean add(final K key, final V value, final double score) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);

			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) {
					return connection.zAdd(rawKey, score, rawValue);
				}
			}, true);
		}

		@Override
		public RedisOperations<K, V> getOperations() {
			return RedisTemplate.this;
		}

		@Override
		public void intersectAndStore(K key, K destKey, K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			final byte[] rawDestKey = rawKey(destKey);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.zInterStore(rawDestKey, rawKeys);
					return null;
				}
			}, true);
		}

		@Override
		public Set<V> range(K key, final int start, final int end) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.zRange(rawKey, start, end);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public Set<V> rangeByScore(K key, final double min, final double max) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.zRangeByScore(rawKey, min, max);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public Integer rank(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);

			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.zRank(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public Integer reverseRank(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);

			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.zRevRank(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public boolean remove(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);

			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) {
					return connection.zRem(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public void removeRange(K key, final int start, final int end) {
			final byte[] rawKey = rawKey(key);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.zRemRange(rawKey, start, end);
					return null;
				}
			}, true);
		}

		@Override
		public void removeRangeByScore(K key, final double min, final double max) {
			final byte[] rawKey = rawKey(key);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.zRemRangeByScore(rawKey, min, max);
					return null;
				}
			}, true);
		}

		@Override
		public Set<V> reverseRange(K key, final int start, final int end) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.zRevRange(rawKey, start, end);
				}
			}, true);

			return values(rawValues, Set.class);
		}

		@Override
		public Double score(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);

			return execute(new RedisCallback<Double>() {
				@Override
				public Double doInRedis(RedisConnection connection) {
					return connection.zScore(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public int size(K key) {
			final byte[] rawKey = rawKey(key);

			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.zCard(rawKey);
				}
			}, true);
		}

		@Override
		public void unionAndStore(K key, K destKey, K... keys) {
			final byte[][] rawKeys = rawKeys(aggregateKeys(key, keys));
			final byte[] rawDestKey = rawKey(destKey);
			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.zUnionStore(rawDestKey, rawKeys);
					return null;
				}
			}, true);
		}
	}


	//
	// Hash Operations
	//

	@Override
	public <HK, HV> BoundHashOperations<K, HK, HV> forHash(K key) {
		return new DefaultBoundHashOperations<K, HK, HV>(key, this);
	}

	@Override
	public <HK, HV> HashOperations<K, HK, HV> hashOps() {
		return new DefaultHashOperations<HK, HV>();
	}

	private class DefaultHashOperations<HK, HV> implements HashOperations<K, HK, HV> {

		@Override
		public RedisOperations<K, ?> getOperations() {
			return RedisTemplate.this;
		}

		@Override
		public HV get(K key, Object hashKey) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawHashKey = rawHashKey(hashKey);

			byte[] rawHashValue = execute(new RedisCallback<byte[]>() {
				@Override
				public byte[] doInRedis(RedisConnection connection) {
					return connection.hGet(rawKey, rawHashKey);
				}
			}, true);

			return deserializeHashValue(rawHashValue);
		}

		@Override
		public Boolean hasKey(K key, Object hashKey) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawHashKey = rawHashKey(hashKey);

			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) {
					return connection.hExists(rawKey, rawHashKey);
				}
			}, true);
		}

		@Override
		public Integer increment(K key, HK hashKey, final int delta) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawHashKey = rawHashKey(hashKey);

			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.hIncrBy(rawKey, rawHashKey, delta);
				}
			}, true);

		}

		@Override
		public Set<HK> keys(K key) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.hKeys(rawKey);
				}
			}, true);

			return (Set<HK>) hashValues(rawValues, Set.class);
		}

		@Override
		public Integer length(K key) {
			final byte[] rawKey = rawKey(key);

			return execute(new RedisCallback<Integer>() {
				@Override
				public Integer doInRedis(RedisConnection connection) {
					return connection.hLen(rawKey);
				}
			}, true);
		}

		@Override
		public void multiSet(K key, Map<? extends HK, ? extends HV> m) {
			if (m.isEmpty()) {
				return;
			}

			final byte[] rawKey = rawKey(key);

			final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(m.size());

			for (Map.Entry<? extends HK, ? extends HV> entry : m.entrySet()) {
				hashes.put(rawHashKey(entry.getKey()), rawHashValue(entry.getValue()));
			}

			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.hMSet(rawKey, hashes);
					return null;
				}
			}, true);
		}

		@Override
		public void set(K key, HK hashKey, HV value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawHashKey = rawHashKey(hashKey);
			final byte[] rawHashValue = rawHashValue(value);

			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.hSet(rawKey, rawHashKey, rawHashValue);
					return null;
				}
			}, true);
		}

		@Override
		public List<HV> values(K key) {
			final byte[] rawKey = rawKey(key);

			List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
				@Override
				public List<byte[]> doInRedis(RedisConnection connection) {
					return connection.hVals(rawKey);
				}
			}, true);

			return (List<HV>) hashValues(rawValues, List.class);
		}

		@Override
		public void delete(K key, Object hashKey) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawHashKey = rawHashKey(hashKey);

			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.hDel(rawKey, rawHashKey);
					return null;
				}
			}, true);
		}
	}
}