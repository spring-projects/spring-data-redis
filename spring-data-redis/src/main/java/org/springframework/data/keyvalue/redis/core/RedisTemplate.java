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
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.SimpleRedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.StringRedisSerializer;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Helper class that simplifies Redis data access code. 
 * <p/>
 * Performs automatic serialization/deserialization between the given objects and the underlying binary data in the Redis store.
 * <p/>
 * The central method is execute, supporting Redis access code implementing the {@link RedisCallback} interface.
 * It provides {@link RedisConnection} handling such that neither the {@link RedisCallback} implementation nor 
 * the calling code needs to explicitly care about retrieving/closing Redis connections, or handling Connection 
 * lifecycle exceptions. For typical single step actions, there are various convenience methods.
 * <p/>
 * Once configured, this class is thread-safe.
 * 
 * <p/>Note that while the template is generified, it is up to the serializers/deserializers to properly convert the given Objects
 * to and from binary data. When using a generic serialization mechanism (such as Java serialization or JSON) the types lose their
 * importance and can be skipped or only used as syntactic sugar. 
 * <p/>
 * <b>This is the central class in Redis support</b>.
 * 
 * @author Costin Leau
 * @param <K> the Redis key type against which the template works (usually a String)
 * @param <V> the Redis value type against which the template works
 */
public class RedisTemplate<K, V> extends RedisAccessor implements RedisOperations<K, V> {

	private boolean exposeConnection = false;
	private RedisSerializer keySerializer = new StringRedisSerializer();
	private RedisSerializer valueSerializer = new SimpleRedisSerializer();
	private RedisSerializer hashKeySerializer = new SimpleRedisSerializer();
	private RedisSerializer hashValueSerializer = new SimpleRedisSerializer();

	/**
	 * Constructs a new <code>RedisTemplate</code> instance.
	 *
	 */
	public RedisTemplate() {
	}

	/**
	 * Constructs a new <code>RedisTemplate</code> instance.
	 *
	 * @param connectionFactory connection factory for creating new connections
	 */
	public RedisTemplate(RedisConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	public <T> T execute(RedisCallback<T> action) {
		return execute(action, isExposeConnection());
	}

	/**
	 * Executes the given action object within a connection, which can be exposed or not.
	 *   
	 * @param <T> return type
	 * @param action callback object that specifies the Redis action
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code 
	 * @return object returned by the action
	 */
	public <T> T execute(RedisCallback<T> action, boolean exposeConnection) {
		return execute(action, exposeConnection, valueSerializer);
	}

	/**
	 * Executes the given action object within a connection, which can be exposed or not. Allows a custom serializer
	 * to be specified for the returned object.
	 * 
	 * @param <T> return type
	 * @param action action callback object that specifies the Redis action
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @param returnSerializer serializer used for converting the binary data to the custom return type
	 * @return returned by the action
	 */
	public <T> T execute(RedisCallback<T> action, boolean exposeConnection, RedisSerializer<?> returnSerializer) {
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
	 * Returns whether to expose the native Redis connection to RedisCallback code, or rather a connection proxy (the default). 
	 *
	 * @return whether to expose the native Redis connection or not
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
	public void setKeySerializer(RedisSerializer<?> serializer) {
		this.keySerializer = serializer;
	}

	/**
	 * Sets the value serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}.
	 * 
	 * @param serializer
	 */
	public void setValueSerializer(RedisSerializer<?> serializer) {
		this.valueSerializer = serializer;
	}

	/**
	 * Sets the hash key (or field) serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}. 
	 * 
	 * @param hashKeySerializer The hashKeySerializer to set.
	 */
	public void setHashKeySerializer(RedisSerializer<?> hashKeySerializer) {
		this.hashKeySerializer = hashKeySerializer;
	}

	/**
	 * Sets the hash value serializer to be used by this template. Defaults to {@link SimpleRedisSerializer}. 
	 * 
	 * @param hashValueSerializer The hashValueSerializer to set.
	 */
	public void setHashValueSerializer(RedisSerializer<?> hashValueSerializer) {
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

	@SuppressWarnings("unchecked")
	private byte[] rawKey(Object key) {
		return (key != null ? keySerializer.serialize(key) : null);
	}

	@SuppressWarnings("unchecked")
	private <T> byte[] rawValue(T value) {
		return (value != null ? valueSerializer.serialize(value) : null);
	}

	private byte[][] rawKeys(Collection<K> keys) {
		final byte[][] rawKeys = new byte[keys.size()][];

		int i = 0;
		for (K key : keys) {
			rawKeys[i++] = rawKey(key);
		}

		return rawKeys;
	}

	private byte[][] rawKeys(K key, Collection<K> keys) {
		final byte[][] rawKeys = new byte[keys.size() + 1][];


		rawKeys[0] = rawKey(key);
		int i = 1;
		for (K k : keys) {
			rawKeys[i++] = rawKey(k);
		}

		return rawKeys;
	}

	@SuppressWarnings("unchecked")
	private <HK> byte[] rawHashKey(HK value) {
		return (value != null ? hashKeySerializer.serialize(value) : null);
	}

	@SuppressWarnings("unchecked")
	private <HV> byte[] rawHashValue(HV value) {
		return (value != null ? hashValueSerializer.serialize(value) : null);
	}


	@SuppressWarnings("unchecked")
	private <T extends Collection<V>> T deserializeValues(Collection<byte[]> rawValues, Class<? extends Collection> type) {
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
	private <H> Collection<H> deserializeHashValues(Collection<byte[]> rawValues, Class<? extends Collection> type) {
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
	private Collection<K> deserializeKeys(Collection<byte[]> rawKeys, Class<? extends Collection> type) {
		Collection<K> values = (List.class.isAssignableFrom(type) ? new ArrayList<K>(rawKeys.size())
				: new LinkedHashSet<K>(rawKeys.size()));
		for (byte[] bs : rawKeys) {
			if (bs != null) {
				values.add((K) hashValueSerializer.deserialize(bs));
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

	@SuppressWarnings( { "unchecked", "unused" })
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
		return serializer.deserialize(value);
	}

	private static boolean isEmpty(byte[] data) {
		return (data == null || data.length == 0);
	}

	// utility methods for the template internal methods
	private abstract class ValueDeserializingRedisCallback implements RedisCallback<V> {
		private Object key;

		public ValueDeserializingRedisCallback(Object key) {
			this.key = key;
		}

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
		return execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.exec();
			}
		});
	}

	@Override
	public void delete(Collection<K> keys) {
		final byte[][] rawKeys = rawKeys(keys);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.del(rawKeys);
				return null;
			}
		}, true);
	}

	@Override
	public Boolean exists(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) {
				return connection.exists(rawKey);
			}
		}, true);
	}

	@Override
	public Boolean expire(K key, long timeout, TimeUnit unit) {
		final byte[] rawKey = rawKey(key);
		final int rawTimeout = (int) unit.toSeconds(timeout);

		return execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) {
				return connection.expire(rawKey, rawTimeout);
			}
		}, true);
	}

	@Override
	public Boolean expireAt(K key, Date date) {
		final byte[] rawKey = rawKey(key);
		final long rawTimeout = date.getTime();

		return execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) {
				return connection.expireAt(rawKey, rawTimeout);
			}
		}, true);
	}

	@Override
	public List<V> sort(K key, final SortParameters params) {
		final byte[] rawKey = rawKey(key);

		List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
			@Override
			public List<byte[]> doInRedis(RedisConnection connection) {
				return connection.sort(rawKey, params);
			}
		}, true);

		return deserializeValues(rawValues, List.class);
	}

	@Override
	public Long sort(K key, final SortParameters params, K destination) {
		final byte[] rawKey = rawKey(key);
		final byte[] rawDestKey = rawKey(destination);

		return execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) {
				return connection.sort(rawKey, params, rawDestKey);
			}
		}, true);
	}


	//
	// Value operations
	//

	@Override
	public Long getExpire(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) {
				return Long.valueOf(connection.ttl(rawKey));
			}
		}, true);
	}

	@Override
	public Set<K> keys(K pattern) {
		final byte[] rawKey = rawKey(pattern);

		Collection<byte[]> rawKeys = execute(new RedisCallback<Collection<byte[]>>() {
			@Override
			public Collection<byte[]> doInRedis(RedisConnection connection) {
				return connection.keys(rawKey);
			}
		}, true);

		return (Set<K>) deserializeKeys(rawKeys, Set.class);
	}

	@Override
	public void persist(K key) {
		final byte[] rawKey = rawKey(key);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.persist(rawKey);
				return null;
			}
		}, true);
	}

	@Override
	public K randomKey() {
		byte[] rawKey = execute(new RedisCallback<byte[]>() {
			@Override
			public byte[] doInRedis(RedisConnection connection) {
				return connection.randomKey();
			}
		}, true);

		return deserializeKey(rawKey);
	}

	@Override
	public void rename(K oldKey, K newKey) {
		final byte[] rawOldKey = rawKey(oldKey);
		final byte[] rawNewKey = rawKey(newKey);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.rename(rawOldKey, rawNewKey);
				return null;
			}
		}, true);
	}

	@Override
	public Boolean renameIfAbsent(K oldKey, K newKey) {
		final byte[] rawOldKey = rawKey(oldKey);
		final byte[] rawNewKey = rawKey(newKey);

		return execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) {
				return connection.renameNX(rawOldKey, rawNewKey);
			}
		}, true);
	}

	@Override
	public DataType type(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<DataType>() {
			@Override
			public DataType doInRedis(RedisConnection connection) {
				return connection.type(rawKey);
			}
		}, true);
	}

	@Override
	public BoundValueOperations<K, V> boundValueOps(K key) {
		return new DefaultBoundValueOperations<K, V>(key, this);
	}

	@Override
	public ValueOperations<K, V> getValueOps() {
		return new DefaultValueOperations();
	}

	private class DefaultValueOperations implements ValueOperations<K, V> {

		@Override
		public V get(final Object key) {

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
		public Long increment(K key, final long delta) {
			final byte[] rawKey = rawKey(key);
			// TODO add conversion service in here ?
			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
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
		public Collection<V> multiGet(Collection<K> keys) {
			if (keys.isEmpty()) {
				return Collections.emptyList();
			}

			final byte[][] rawKeys = new byte[keys.size()][];

			int counter = 0;
			for (K hashKey : keys) {
				rawKeys[counter++] = rawKey(hashKey);
			}

			List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
				@Override
				public List<byte[]> doInRedis(RedisConnection connection) {
					return connection.mGet(rawKeys);
				}
			}, true);

			return (List<V>) deserializeValues(rawValues, List.class);
		}

		@Override
		public void multiSet(Map<? extends K, ? extends V> m) {
			if (m.isEmpty()) {
				return;
			}

			final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());

			for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
				rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
			}

			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.mSet(rawKeys);
					return null;
				}
			}, true);
		}

		@Override
		public void multiSetIfAbsent(Map<? extends K, ? extends V> m) {
			if (m.isEmpty()) {
				return;
			}

			final Map<byte[], byte[]> rawKeys = new LinkedHashMap<byte[], byte[]>(m.size());

			for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
				rawKeys.put(rawKey(entry.getKey()), rawValue(entry.getValue()));
			}

			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) {
					connection.mSetNX(rawKeys);
					return null;
				}
			}, true);
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
		public void set(K key, V value, long timeout, TimeUnit unit) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			final long rawTimeout = unit.toSeconds(timeout);

			execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) throws DataAccessException {
					connection.setEx(rawKey, (int) rawTimeout, rawValue);
					return null;
				}
			}, true);
		}

		@Override
		public Boolean setIfAbsent(K key, V value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);

			return execute(new RedisCallback<Boolean>() {
				@Override
				public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
					return connection.setNX(rawKey, rawValue);
				}
			}, true);
		}
	}

	@Override
	public ListOperations<K, V> getListOps() {
		return new DefaultListOperations();
	}

	@Override
	public BoundListOperations<K, V> boundListOps(K key) {
		return new DefaultBoundListOperations<K, V>(key, this);
	}


	@Override
	public void multi() {
		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.multi();
				return null;
			}
		}, true);
	}

	@Override
	public void watch(Collection<K> keys) {
		final byte[][] rawKeys = rawKeys(keys);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.watch(rawKeys);
				return null;
			}
		}, true);
	}

	//
	// List operations
	//

	private class DefaultListOperations implements ListOperations<K, V> {

		@Override
		public V index(K key, final long index) {
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
		public V leftPop(K key, long timeout, TimeUnit unit) {
			final int tm = (int) unit.toSeconds(timeout);

			return execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					return connection.bLPop(tm, rawKey).get(0);
				}
			}, true);
		}



		@Override
		public Long leftPush(K key, V value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					return connection.lPush(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public Long size(K key) {
			final byte[] rawKey = rawKey(key);
			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					return connection.lLen(rawKey);
				}
			}, true);
		}

		@Override
		public List<V> range(K key, final long start, final long end) {
			final byte[] rawKey = rawKey(key);
			return execute(new RedisCallback<List<V>>() {
				@Override
				public List<V> doInRedis(RedisConnection connection) {
					return deserializeValues(connection.lRange(rawKey, start, end), List.class);
				}
			}, true);
		}

		@Override
		public Long remove(K key, final long count, Object value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
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
		public V rightPop(K key, long timeout, TimeUnit unit) {
			final int tm = (int) unit.toSeconds(timeout);

			return execute(new ValueDeserializingRedisCallback(key) {
				@Override
				protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
					return connection.bRPop(tm, rawKey).get(0);
				}
			}, true);
		}

		@Override
		public Long rightPush(K key, V value) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(value);
			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					return connection.rPush(rawKey, rawValue);
				}
			}, true);
		}

		@Override
		public void set(K key, final long index, V value) {
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
		public void trim(K key, final long start, final long end) {
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

	@Override
	public BoundSetOperations<K, V> boundSetOps(K key) {
		return new DefaultBoundSetOperations<K, V>(key, this);
	}

	@Override
	public SetOperations<K, V> getSetOps() {
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
		public Set<V> diff(final K key, final Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sDiff(rawKeys);
				}
			}, true);

			return deserializeValues(rawValues, Set.class);
		}

		@Override
		public void diffAndStore(final K key, K destKey, final Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
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
		public Set<V> intersect(K key, Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sInter(rawKeys);
				}
			}, true);

			return deserializeValues(rawValues, Set.class);
		}

		@Override
		public void intersectAndStore(K key, K destKey, Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
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
		public Boolean isMember(K key, Object o) {
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

			return deserializeValues(rawValues, Set.class);
		}

		@Override
		public Boolean remove(K key, Object o) {
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
		public Long size(K key) {
			final byte[] rawKey = rawKey(key);
			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					return connection.sCard(rawKey);
				}
			}, true);
		}

		@Override
		public Set<V> union(K key, Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.sUnion(rawKeys);
				}
			}, true);

			return deserializeValues(rawValues, Set.class);
		}

		@Override
		public void unionAndStore(K key, K destKey, Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
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
	public BoundZSetOperations<K, V> boundZSetOps(K key) {
		return new DefaultBoundZSetOperations<K, V>(key, this);
	}

	@Override
	public ZSetOperations<K, V> getZSetOps() {
		return new DefaultZSetOperations();
	}

	private class DefaultZSetOperations implements ZSetOperations<K, V> {

		@Override
		public Boolean add(final K key, final V value, final double score) {
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
		public void intersectAndStore(K key, K destKey, Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
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
		public Set<V> range(K key, final long start, final long end) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.zRange(rawKey, start, end);
				}
			}, true);

			return deserializeValues(rawValues, Set.class);
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

			return deserializeValues(rawValues, Set.class);
		}

		@Override
		public Long rank(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);

			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					Long zRank = connection.zRank(rawKey, rawValue);
					return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
				}
			}, true);
		}

		@Override
		public Long reverseRank(K key, Object o) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawValue = rawValue(o);

			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					Long zRank = connection.zRevRank(rawKey, rawValue);
					return (zRank != null && zRank.longValue() >= 0 ? zRank : null);
				}
			}, true);
		}

		@Override
		public Boolean remove(K key, Object o) {
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
		public void removeRange(K key, final long start, final long end) {
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
		public Set<V> reverseRange(K key, final long start, final long end) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.zRevRange(rawKey, start, end);
				}
			}, true);

			return deserializeValues(rawValues, Set.class);
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
		public Long size(K key) {
			final byte[] rawKey = rawKey(key);

			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					return connection.zCard(rawKey);
				}
			}, true);
		}

		@Override
		public void unionAndStore(K key, K destKey, Collection<K> keys) {
			final byte[][] rawKeys = rawKeys(key, keys);
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
	public <HK, HV> BoundHashOperations<K, HK, HV> boundHashOps(K key) {
		return new DefaultBoundHashOperations<K, HK, HV>(key, this);
	}

	@Override
	public <HK, HV> HashOperations<K, HK, HV> getHashOps() {
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

			return RedisTemplate.this.<HV> deserializeHashValue(rawHashValue);
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
		public Long increment(K key, HK hashKey, final long delta) {
			final byte[] rawKey = rawKey(key);
			final byte[] rawHashKey = rawHashKey(hashKey);

			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
					return connection.hIncrBy(rawKey, rawHashKey, delta);
				}
			}, true);

		}

		@SuppressWarnings("unchecked")
		@Override
		public Set<HK> keys(K key) {
			final byte[] rawKey = rawKey(key);

			Set<byte[]> rawValues = execute(new RedisCallback<Set<byte[]>>() {
				@Override
				public Set<byte[]> doInRedis(RedisConnection connection) {
					return connection.hKeys(rawKey);
				}
			}, true);

			return (Set<HK>) deserializeHashValues(rawValues, Set.class);
		}

		@Override
		public Long size(K key) {
			final byte[] rawKey = rawKey(key);

			return execute(new RedisCallback<Long>() {
				@Override
				public Long doInRedis(RedisConnection connection) {
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


		@SuppressWarnings("unchecked")
		@Override
		public Collection<HV> multiGet(K key, Collection<HK> fields) {
			if (fields.isEmpty()) {
				return Collections.emptyList();
			}

			final byte[] rawKey = rawKey(key);

			final byte[][] rawHashKeys = new byte[fields.size()][];

			int counter = 0;
			for (HK hashKey : fields) {
				rawHashKeys[counter++] = rawHashKey(hashKey);
			}

			List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
				@Override
				public List<byte[]> doInRedis(RedisConnection connection) {
					return connection.hMGet(rawKey, rawHashKeys);
				}
			}, true);

			return (List<HV>) deserializeHashValues(rawValues, List.class);
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

		@SuppressWarnings("unchecked")
		@Override
		public List<HV> values(K key) {
			final byte[] rawKey = rawKey(key);

			List<byte[]> rawValues = execute(new RedisCallback<List<byte[]>>() {
				@Override
				public List<byte[]> doInRedis(RedisConnection connection) {
					return connection.hVals(rawKey);
				}
			}, true);

			return (List<HV>) deserializeHashValues(rawValues, List.class);
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