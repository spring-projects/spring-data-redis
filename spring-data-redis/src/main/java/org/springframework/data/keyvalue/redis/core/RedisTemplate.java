/*
 * Copyright 2010-2011 the original author or authors.
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

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.RedisConnection;
import org.springframework.data.keyvalue.redis.connection.RedisConnectionFactory;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.core.query.QueryUtils;
import org.springframework.data.keyvalue.redis.core.query.SortQuery;
import org.springframework.data.keyvalue.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.RedisSerializer;
import org.springframework.data.keyvalue.redis.serializer.SerializationUtils;
import org.springframework.data.keyvalue.redis.serializer.StringRedisSerializer;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Helper class that simplifies Redis data access code. 
 * <p/>
 * Performs automatic serialization/deserialization between the given objects and the underlying binary data in the Redis store.
 * By default, it uses Java serialization for its objects (through {@link JdkSerializationRedisSerializer}). For String intensive
 * operations consider the dedicated {@link StringRedisTemplate}.
 * <p/>
 * The central method is execute, supporting Redis access code implementing the {@link RedisCallback} interface.
 * It provides {@link RedisConnection} handling such that neither the {@link RedisCallback} implementation nor 
 * the calling code needs to explicitly care about retrieving/closing Redis connections, or handling Connection 
 * lifecycle exceptions. For typical single step actions, there are various convenience methods.
 * <p/>
 * Once configured, this class is thread-safe.
 * 
 * <p/>Note that while the template is generified, it is up to the serializers/deserializers to properly convert the given Objects
 * to and from binary data. 
 * <p/>
 * <b>This is the central class in Redis support</b>.
 * 
 * @author Costin Leau
 * @param <K> the Redis key type against which the template works (usually a String)
 * @param <V> the Redis value type against which the template works
 * @see StringRedisTemplate
 */
public class RedisTemplate<K, V> extends RedisAccessor implements RedisOperations<K, V> {

	private boolean exposeConnection = false;
	private RedisSerializer<?> defaultSerializer = new JdkSerializationRedisSerializer();

	private RedisSerializer keySerializer = null;
	private RedisSerializer valueSerializer = null;
	private RedisSerializer hashKeySerializer = null;
	private RedisSerializer hashValueSerializer = null;
	private RedisSerializer<String> stringSerializer = new StringRedisSerializer();

	// cache singleton objects (where possible)
	private ValueOperations<K, V> valueOps;
	private ListOperations<K, V> listOps;
	private SetOperations<K, V> setOps;
	private ZSetOperations<K, V> zSetOps;

	/**
	 * Constructs a new <code>RedisTemplate</code> instance.
	 *
	 */
	public RedisTemplate() {
	}

	/**
	 * Constructs a new <code>RedisTemplate</code> instance and automatically initializes the template.
	 * If other parameters need to be set, it is recommended to use {@link #setConnectionFactory(RedisConnectionFactory)} instead.
	 *
	 * @param connectionFactory connection factory for creating new connections
	 */
	public RedisTemplate(RedisConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}


	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		boolean defaultUsed = false;

		if (keySerializer == null) {
			keySerializer = defaultSerializer;
			defaultUsed = true;
		}
		if (valueSerializer == null) {
			valueSerializer = defaultSerializer;
			defaultUsed = true;
		}

		if (hashKeySerializer == null) {
			hashKeySerializer = defaultSerializer;
			defaultUsed = true;
		}

		if (hashValueSerializer == null) {
			hashValueSerializer = defaultSerializer;
			defaultUsed = true;
		}

		if (defaultUsed) {
			Assert.notNull(defaultSerializer, "default serializer null and not all serializers initialized");
		}

		valueOps = new DefaultValueOperations<K, V>(this);
		listOps = new DefaultListOperations<K, V>(this);
		setOps = new DefaultSetOperations<K, V>(this);
		zSetOps = new DefaultZSetOperations<K, V>(this);
	}

	@Override
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
		return execute(action, exposeConnection, false);
	}

	/**
	 * Executes the given action object within a connection that can be exposed or not. Additionally, the connection
	 * can be pipelined. Note the results of the pipeline are discarded (making it suitable for write-only scenarios).
	 * Use {@link #executePipelined(RedisCallback)} as an alternative.
	 * 
	 * @param <T> return type
	 * @param action callback object to execute
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @param pipeline whether to pipeline or not the connection for the execution 
	 * @return object returned by the action
	 */
	public <T> T execute(RedisCallback<T> action, boolean exposeConnection, boolean pipeline) {
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		RedisConnection conn = RedisConnectionUtils.getConnection(factory);

		boolean existingConnection = TransactionSynchronizationManager.hasResource(factory);
		preProcessConnection(conn, existingConnection);

		boolean pipelineStatus = conn.isPipelined();
		if (pipeline && !pipelineStatus) {
			conn.openPipeline();
		}

		try {
			RedisConnection connToExpose = (exposeConnection ? conn : createRedisConnectionProxy(conn));
			T result = action.doInRedis(connToExpose);
			// TODO: any other connection processing?
			return postProcessResult(result, conn, existingConnection);
		} finally {
			try {
				if (pipeline && !pipelineStatus) {
					conn.closePipeline();
				}
			} finally {
				RedisConnectionUtils.releaseConnection(conn, factory);
			}
		}
	}


	@Override
	public <T> T execute(SessionCallback<T> session) {
		RedisConnectionFactory factory = getConnectionFactory();
		// bind connection
		RedisConnectionUtils.bindConnection(factory);
		try {
			return session.execute(this);
		} finally {
			RedisConnectionUtils.unbindConnection(factory);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<V> executePipelined(final RedisCallback<?> action) {
		return executePipelined(action, valueSerializer);
	}

	/**
	 * Executes the given action object on a pipelined connection, returning the results using a dedicated serializer.
	 * Note that the callback <b>cannot</b> return a non-null value as it gets overwritten by the pipeline.
	 * 
	 * @param action callback object to execute
	 * @param resultSerializer
	 * @return list of objects returned by the pipeline
	 */
	public <T> List<T> executePipelined(final RedisCallback<?> action, final RedisSerializer<T> resultSerializer) {
		return execute(new RedisCallback<List<T>>() {
			public List<T> doInRedis(RedisConnection connection) throws DataAccessException {
				connection.openPipeline();
				Object result = action.doInRedis(connection);
				if (result != null) {
					throw new InvalidDataAccessApiUsageException(
							"Callback cannot returned a non-null value as it gets overwritten by the pipeline");
				}
				List<byte[]> pipeline = connection.closePipeline();
				return SerializationUtils.deserialize(pipeline, resultSerializer);
			}
		});
	}

	protected RedisConnection createRedisConnectionProxy(RedisConnection pm) {
		Class<?>[] ifcs = ClassUtils.getAllInterfacesForClass(pm.getClass(), getClass().getClassLoader());
		return (RedisConnection) Proxy.newProxyInstance(pm.getClass().getClassLoader(), ifcs,
				new CloseSuppressingInvocationHandler(pm));
	}

	/**
	 * Processes the connection (before any settings are executed on it). Default implementation returns the connection as is.
	 * 
	 * @param connection redis connection
	 */
	protected RedisConnection preProcessConnection(RedisConnection connection, boolean existingConnection) {
		return connection;
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
	 * Returns the default serializer used by this template.
	 * 
	 * @return template default serializer
	 */
	public RedisSerializer<?> getDefaultSerializer() {
		return defaultSerializer;
	}

	/**
	 * Sets the default serializer to use for this template. All serializers (expect the {@link #setStringSerializer(RedisSerializer)}) are
	 * initialized to this value unless explicitly set. Defaults to {@link JdkSerializationRedisSerializer}.
	 * 
	 * @param serializer default serializer to use
	 */
	public void setDefaultSerializer(RedisSerializer<?> serializer) {
		this.defaultSerializer = serializer;
	}

	/**
	 * Sets the key serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}.
	 * 
	 * @param serializer the key serializer to be used by this template.
	 */
	public void setKeySerializer(RedisSerializer<?> serializer) {
		this.keySerializer = serializer;
	}

	/**
	 * Returns the key serializer used by this template.
	 * 
	 * @return the key serializer used by this template.
	 */
	public RedisSerializer<?> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Sets the value serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}.
	 * 
	 * @param serializer the value serializer to be used by this template.
	 */
	public void setValueSerializer(RedisSerializer<?> serializer) {
		this.valueSerializer = serializer;
	}

	/**
	 * Returns the value serializer used by this template.
	 * 
	 * @return the value serializer used by this template.
	 */
	public RedisSerializer<?> getValueSerializer() {
		return valueSerializer;
	}

	/**
	 * Returns the hashKeySerializer.
	 *
	 * @return Returns the hashKeySerializer
	 */
	public RedisSerializer<?> getHashKeySerializer() {
		return hashKeySerializer;
	}

	/**
	 * Sets the hash key (or field) serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}. 
	 * 
	 * @param hashKeySerializer The hashKeySerializer to set.
	 */
	public void setHashKeySerializer(RedisSerializer<?> hashKeySerializer) {
		this.hashKeySerializer = hashKeySerializer;
	}

	/**
	 * Returns the hashValueSerializer.
	 *
	 * @return Returns the hashValueSerializer
	 */
	public RedisSerializer<?> getHashValueSerializer() {
		return hashValueSerializer;
	}

	/**
	 * Sets the hash value serializer to be used by this template. Defaults to {@link #getDefaultSerializer()}. 
	 * 
	 * @param hashValueSerializer The hashValueSerializer to set.
	 */
	public void setHashValueSerializer(RedisSerializer<?> hashValueSerializer) {
		this.hashValueSerializer = hashValueSerializer;
	}

	/**
	 * Returns the stringSerializer.
	 *
	 * @return Returns the stringSerializer
	 */
	public RedisSerializer<String> getStringSerializer() {
		return stringSerializer;
	}

	/**
	 * Sets the string value serializer to be used by this template (when the arguments or return types
	 * are always strings). Defaults to {@link StringRedisSerializer}.
	 * 
	 * @see ValueOperations#get(Object, int, int)
	 * @param stringSerializer The stringValueSerializer to set.
	 */
	public void setStringSerializer(RedisSerializer<String> stringSerializer) {
		this.stringSerializer = stringSerializer;
	}

	@SuppressWarnings("unchecked")
	private byte[] rawKey(Object key) {
		Assert.notNull(key, "non null key required");
		return keySerializer.serialize(key);
	}

	private byte[] rawString(String key) {
		return stringSerializer.serialize(key);
	}

	@SuppressWarnings("unchecked")
	private byte[] rawValue(Object value) {
		return valueSerializer.serialize(value);
	}

	private byte[][] rawKeys(Collection<K> keys) {
		final byte[][] rawKeys = new byte[keys.size()][];

		int i = 0;
		for (K key : keys) {
			rawKeys[i++] = rawKey(key);
		}

		return rawKeys;
	}

	@SuppressWarnings("unchecked")
	private K deserializeKey(byte[] value) {
		return (K) keySerializer.deserialize(value);
	}

	//
	// RedisOperations
	//
	@Override
	public List<Object> exec() {
		return execute(new RedisCallback<List<Object>>() {

			@Override
			public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.exec();
			}
		});
	}

	@Override
	public void delete(K key) {
		final byte[] rawKey = rawKey(key);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.del(rawKey);
				return null;
			}
		}, true);
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
	public Boolean hasKey(K key) {
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
	public void convertAndSend(String channel, Object message) {
		Assert.hasText(channel, "a non-empty channel is required");

		final byte[] rawChannel = rawString(channel);
		final byte[] rawMessage = rawValue(message);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.publish(rawChannel, rawMessage);
				return null;
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

	@SuppressWarnings("unchecked")
	@Override
	public Set<K> keys(K pattern) {
		final byte[] rawKey = rawKey(pattern);

		Collection<byte[]> rawKeys = execute(new RedisCallback<Collection<byte[]>>() {
			@Override
			public Collection<byte[]> doInRedis(RedisConnection connection) {
				return connection.keys(rawKey);
			}
		}, true);

		return (Set<K>) SerializationUtils.deserialize(rawKeys, keySerializer);
	}

	@Override
	public Boolean persist(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Boolean>() {
			@Override
			public Boolean doInRedis(RedisConnection connection) {
				return connection.persist(rawKey);
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
	public void discard() {
		execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.discard();
				return null;
			}
		}, true);
	}

	@Override
	public void watch(K key) {
		final byte[] rawKey = rawKey(key);

		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) {
				connection.watch(rawKey);
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

	@Override
	public void unwatch() {
		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.unwatch();
				return null;
			}
		}, true);
	}

	// Sort operations

	@SuppressWarnings("unchecked")
	@Override
	public List<V> sort(SortQuery<K> query) {
		return sort(query, valueSerializer);
	}

	@Override
	public <T> List<T> sort(SortQuery<K> query, RedisSerializer<T> resultSerializer) {
		final byte[] rawKey = rawKey(query.getKey());
		final SortParameters params = QueryUtils.convertQuery(query, stringSerializer);

		List<byte[]> vals = execute(new RedisCallback<List<byte[]>>() {
			@Override
			public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sort(rawKey, params);
			}
		}, true);

		return SerializationUtils.deserialize(vals, resultSerializer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> sort(SortQuery<K> query, BulkMapper<T, V> bulkMapper) {
		return sort(query, bulkMapper, valueSerializer);
	}

	@Override
	public <T, S> List<T> sort(SortQuery<K> query, BulkMapper<T, S> bulkMapper, RedisSerializer<S> resultSerializer) {
		List<S> values = sort(query, resultSerializer);

		int bulkSize = query.getGetPattern().size();
		List<T> result = new ArrayList<T>(values.size() / bulkSize + 1);

		List<S> bulk = new ArrayList<S>(bulkSize);
		for (S s : values) {

			bulk.add(s);
			if (bulk.size() == bulkSize) {
				result.add(bulkMapper.mapBulk(Collections.unmodifiableList(bulk)));
				// create a new list (we could reuse the old one but the client might hang on to it for some reason)
				bulk = new ArrayList<S>(bulkSize);
			}
		}

		return result;
	}

	@Override
	public Long sort(SortQuery<K> query, K storeKey) {
		final byte[] rawStoreKey = rawKey(storeKey);
		final byte[] rawKey = rawKey(query.getKey());
		final SortParameters params = QueryUtils.convertQuery(query, stringSerializer);

		return execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sort(rawKey, params, rawStoreKey);
			}
		}, true);
	}

	@Override
	public BoundValueOperations<K, V> boundValueOps(K key) {
		return new DefaultBoundValueOperations<K, V>(key, this);
	}

	@Override
	public ValueOperations<K, V> opsForValue() {
		return valueOps;
	}

	@Override
	public ListOperations<K, V> opsForList() {
		return listOps;
	}

	@Override
	public BoundListOperations<K, V> boundListOps(K key) {
		return new DefaultBoundListOperations<K, V>(key, this);
	}

	@Override
	public BoundSetOperations<K, V> boundSetOps(K key) {
		return new DefaultBoundSetOperations<K, V>(key, this);
	}

	@Override
	public SetOperations<K, V> opsForSet() {
		return setOps;
	}

	@Override
	public BoundZSetOperations<K, V> boundZSetOps(K key) {
		return new DefaultBoundZSetOperations<K, V>(key, this);
	}

	@Override
	public ZSetOperations<K, V> opsForZSet() {
		return zSetOps;
	}

	@Override
	public <HK, HV> BoundHashOperations<K, HK, HV> boundHashOps(K key) {
		return new DefaultBoundHashOperations<K, HK, HV>(key, this);
	}

	@Override
	public <HK, HV> HashOperations<K, HK, HV> opsForHash() {
		return new DefaultHashOperations<K, HK, HV>(this);
	}
}