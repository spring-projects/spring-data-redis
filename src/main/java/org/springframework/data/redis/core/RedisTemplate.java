/*
 * Copyright 2011-2013 the original author or authors.
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

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.query.QueryUtils;
import org.springframework.data.redis.core.query.SortQuery;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.script.ScriptExecutor;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

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
	private boolean initialized = false;
	private boolean enableDefaultSerializer = true;
	private RedisSerializer<?> defaultSerializer = new JdkSerializationRedisSerializer();

	private RedisSerializer keySerializer = null;
	private RedisSerializer valueSerializer = null;
	private RedisSerializer hashKeySerializer = null;
	private RedisSerializer hashValueSerializer = null;
	private RedisSerializer<String> stringSerializer = new StringRedisSerializer();

	private ScriptExecutor<K> scriptExecutor;

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


	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		boolean defaultUsed = false;

		if(enableDefaultSerializer) {
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
		}

		if (enableDefaultSerializer && defaultUsed) {
			Assert.notNull(defaultSerializer, "default serializer null and not all serializers initialized");
		}

		if(scriptExecutor == null) {
			this.scriptExecutor = new DefaultScriptExecutor<K>(this);
		}

		initialized = true;
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
		return execute(action, exposeConnection, false);
	}

	/**
	 * Executes the given action object within a connection that can be exposed or not. Additionally, the connection
	 * can be pipelined. Note the results of the pipeline are discarded (making it suitable for write-only scenarios).
	 * 
	 * @param <T> return type
	 * @param action callback object to execute
	 * @param exposeConnection whether to enforce exposure of the native Redis Connection to callback code
	 * @param pipeline whether to pipeline or not the connection for the execution 
	 * @return object returned by the action
	 */
	public <T> T execute(RedisCallback<T> action, boolean exposeConnection, boolean pipeline) {
		Assert.isTrue(initialized, "template not initialized; call afterPropertiesSet() before using it");
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		RedisConnection conn = null;
		try {
			conn = RedisConnectionUtils.getConnection(factory);

			boolean existingConnection = TransactionSynchronizationManager.hasResource(factory);

			RedisConnection connToUse = preProcessConnection(conn, existingConnection);

			boolean pipelineStatus = connToUse.isPipelined();
			if (pipeline && !pipelineStatus) {
				connToUse.openPipeline();
			}

			RedisConnection connToExpose = (exposeConnection ? connToUse : createRedisConnectionProxy(connToUse));
			T result = action.doInRedis(connToExpose);

			// close pipeline
			if (pipeline && !pipelineStatus) {
				connToUse.closePipeline();
			}

			// TODO: any other connection processing?
			return postProcessResult(result, connToUse, existingConnection);
		} finally {
			RedisConnectionUtils.releaseConnection(conn, factory);
		}
	}



	public <T> T execute(SessionCallback<T> session) {
		Assert.isTrue(initialized, "template not initialized; call afterPropertiesSet() before using it");
		Assert.notNull(session, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		// bind connection
		RedisConnectionUtils.bindConnection(factory);
		try {
			return session.execute(this);
		} finally {
			RedisConnectionUtils.unbindConnection(factory);
		}
	}

	public List<Object> executePipelined(final SessionCallback<?> session) {
		return executePipelined(session, valueSerializer);
	}

	public List<Object> executePipelined(final SessionCallback<?> session, final RedisSerializer<?> resultSerializer) {
		Assert.isTrue(initialized, "template not initialized; call afterPropertiesSet() before using it");
		Assert.notNull(session, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		// bind connection
		RedisConnectionUtils.bindConnection(factory);
		try {
			return execute(new RedisCallback<List<Object>>() {
				public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
					connection.openPipeline();
					boolean pipelinedClosed = false;
					try {
						Object result = executeSession(session);
						if (result != null) {
							throw new InvalidDataAccessApiUsageException(
								"Callback cannot return a non-null value as it gets overwritten by the pipeline");
						}
						List<Object> closePipeline = connection.closePipeline();
						pipelinedClosed = true;
						return deserializeMixedResults(closePipeline, resultSerializer,
								hashKeySerializer, hashValueSerializer);
					} finally {
						if (!pipelinedClosed) {
							connection.closePipeline();
						}
					}
				}
			});
		} finally {
			RedisConnectionUtils.unbindConnection(factory);
		}
	}

	public List<Object> executePipelined(final RedisCallback<?> action) {
		return executePipelined(action, valueSerializer);
	}

	public List<Object> executePipelined(final RedisCallback<?> action, final RedisSerializer<?> resultSerializer) {
		return execute(new RedisCallback<List<Object>>() {
			public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
				connection.openPipeline();
				boolean pipelinedClosed = false;
				try {
					Object result = action.doInRedis(connection);
					if (result != null) {
						throw new InvalidDataAccessApiUsageException(
							"Callback cannot return a non-null value as it gets overwritten by the pipeline");
					}
					List<Object> closePipeline = connection.closePipeline();
					pipelinedClosed = true;
					return deserializeMixedResults(closePipeline, resultSerializer,
							resultSerializer, resultSerializer);
				} finally {
					if (!pipelinedClosed) {
						connection.closePipeline();
					}
				}
			}
		});
	}

	public <T> T execute(RedisScript<T> script, List<K> keys, Object... args) {
		return scriptExecutor.execute(script, keys, args);
	}

	public <T> T execute(RedisScript<T> script, RedisSerializer<?> argsSerializer, RedisSerializer<T> resultSerializer,
			List<K> keys, Object... args) {
		return scriptExecutor.execute(script, argsSerializer, resultSerializer, keys, args);
	}

	private Object executeSession(SessionCallback<?> session) {
		return session.execute(this);
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
	 *
	 * @return Whether or not the default serializer should be used. If not, any serializers not explicilty set
	 * will remain null and values will not be serialized or deserialized.
	 */
	public boolean isEnableDefaultSerializer() {
		return enableDefaultSerializer;
	}

	/**
	 *
	 * @param enableDefaultSerializer Whether or not the default serializer should be used. If not,
	 * any serializers not explicilty set will remain null and values will not be serialized or deserialized.
	 */
	public void setEnableDefaultSerializer(boolean enableDefaultSerializer) {
		this.enableDefaultSerializer = enableDefaultSerializer;
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
	 * @see ValueOperations#get(Object, long, long)
	 * @param stringSerializer The stringValueSerializer to set.
	 */
	public void setStringSerializer(RedisSerializer<String> stringSerializer) {
		this.stringSerializer = stringSerializer;
	}

	/**
	 *
	 * @param scriptExecutor The {@link ScriptExecutor} to use for executing Redis scripts
	 */
	public void setScriptExecutor(ScriptExecutor<K> scriptExecutor) {
		this.scriptExecutor = scriptExecutor;
	}

	@SuppressWarnings("unchecked")
	private byte[] rawKey(Object key) {
		Assert.notNull(key, "non null key required");
		if(keySerializer == null && key instanceof byte[]) {
			return (byte[]) key;
		}
		return keySerializer.serialize(key);
	}

	private byte[] rawString(String key) {
		return stringSerializer.serialize(key);
	}

	@SuppressWarnings("unchecked")
	private byte[] rawValue(Object value) {
		if(valueSerializer == null && value instanceof byte[]) {
			return (byte[]) value;
		}
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
		return keySerializer != null ? (K) keySerializer.deserialize(value) : (K) value;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Object> deserializeMixedResults(List<Object> rawValues, RedisSerializer valueSerializer,
			RedisSerializer hashKeySerializer, RedisSerializer hashValueSerializer) {
		if(rawValues == null) {
			return null;
		}
		List<Object> values = new ArrayList<Object>();
		for(Object rawValue: rawValues) {
			if(rawValue instanceof byte[] && valueSerializer != null) {
				values.add(valueSerializer.deserialize((byte[])rawValue));
			} else if(rawValue instanceof List) {
				// Lists are the only potential Collections of mixed values....
				values.add(deserializeMixedResults((List)rawValue, valueSerializer, hashKeySerializer, hashValueSerializer));
			} else if(rawValue instanceof Set && !(((Set)rawValue).isEmpty())) {
				values.add(deserializeSet((Set)rawValue, valueSerializer));
			} else if(rawValue instanceof Map && !(((Map)rawValue).isEmpty()) &&
					((Map)rawValue).values().iterator().next() instanceof byte[]) {
				values.add(SerializationUtils.deserialize((Map)rawValue, hashKeySerializer, hashValueSerializer));
			} else {
				values.add(rawValue);
			}
		}
		return values;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Set<?> deserializeSet(Set rawSet, RedisSerializer valueSerializer) {
		if(rawSet.isEmpty()) {
			return rawSet;
		}
		Object setValue = rawSet.iterator().next();
		if(setValue instanceof byte[] && valueSerializer != null) {
			return (SerializationUtils.deserialize((Set)rawSet, valueSerializer));
		}else if(setValue instanceof Tuple) {
			return convertTupleValues(rawSet, valueSerializer);
		} else {
			return rawSet;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Set<TypedTuple<V>> convertTupleValues(Set<Tuple> rawValues, RedisSerializer valueSerializer) {
		Set<TypedTuple<V>> set = new LinkedHashSet<TypedTuple<V>>(rawValues.size());
		for (Tuple rawValue : rawValues) {
			Object value = rawValue.getValue();
			if(valueSerializer != null) {
				value = valueSerializer.deserialize(rawValue.getValue());
			}
			set.add(new DefaultTypedTuple(value, rawValue.getScore()));
		}
		return set;
	}

	//
	// RedisOperations
	//

	/**
	 * Execute a transaction, using the default {@link RedisSerializer}s to deserialize
	 * any results that are byte[]s or Collections or Maps of byte[]s or Tuples. Other result
	 * types (Long, Boolean, etc) are left as-is in the converted results.
	 *
	 * If conversion of tx results has been disabled in the {@link RedisConnectionFactory},
	 * the results of exec will be returned without deserialization. This check is mostly for
	 * backwards compatibility with 1.0.
	 *
	 * @return The (possibly deserialized) results of transaction exec
	 */
	public List<Object> exec() {
		List<Object> results = execRaw();
		if(getConnectionFactory().getConvertPipelineAndTxResults()) {
			return deserializeMixedResults(results, valueSerializer,
					hashKeySerializer, hashValueSerializer);
		} else {
			return results;
		}
	}

	public List<Object> exec(RedisSerializer<?> valueSerializer) {
		return deserializeMixedResults(execRaw(), valueSerializer, valueSerializer,
				valueSerializer);
	}

	protected List<Object> execRaw() {
		return execute(new RedisCallback<List<Object>>() {
			public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.exec();
			}
		});
	}


	public void delete(K key) {
		final byte[] rawKey = rawKey(key);

		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) {
				connection.del(rawKey);
				return null;
			}
		}, true);
	}


	public void delete(Collection<K> keys) {
		if (CollectionUtils.isEmpty(keys)) {
			return;
		}

		final byte[][] rawKeys = rawKeys(keys);

		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) {
				connection.del(rawKeys);
				return null;
			}
		}, true);
	}


	public Boolean hasKey(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				return connection.exists(rawKey);
			}
		}, true);
	}


	public Boolean expire(K key, final long timeout, final TimeUnit unit) {
		final byte[] rawKey = rawKey(key);
		final long rawTimeout = TimeoutUtils.toMillis(timeout, unit);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				try {
					return connection.pExpire(rawKey, rawTimeout);
				} catch(Exception e) {
					// Driver may not support pExpire or we may be running on Redis 2.4
					return connection.expire(rawKey, TimeoutUtils.toSeconds(timeout, unit));
				}
			}
		}, true);
	}


	public Boolean expireAt(K key, final Date date) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				try {
					return connection.pExpireAt(rawKey, date.getTime());
				} catch(Exception e) {
					return connection.expireAt(rawKey, date.getTime() / 1000);
				}
			}
		}, true);
	}


	public void convertAndSend(String channel, Object message) {
		Assert.hasText(channel, "a non-empty channel is required");

		final byte[] rawChannel = rawString(channel);
		final byte[] rawMessage = rawValue(message);

		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) {
				connection.publish(rawChannel, rawMessage);
				return null;
			}
		}, true);
	}


	//
	// Value operations
	//


	public Long getExpire(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				return connection.ttl(rawKey);
			}
		}, true);
	}

	public Long getExpire(K key, final TimeUnit timeUnit) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) {
				try {
					return timeUnit.convert(connection.pTtl(rawKey), TimeUnit.MILLISECONDS);
				} catch(Exception e) {
					// Driver may not support pTtl or we may be running on Redis 2.4
					return timeUnit.convert(connection.ttl(rawKey), TimeUnit.SECONDS);
				}
			}
		}, true);
	}

	@SuppressWarnings("unchecked")
	public Set<K> keys(K pattern) {
		final byte[] rawKey = rawKey(pattern);

		Set<byte[]> rawKeys = execute(new RedisCallback<Set<byte[]>>() {

			public Set<byte[]> doInRedis(RedisConnection connection) {
				return connection.keys(rawKey);
			}
		}, true);

		return keySerializer != null ? SerializationUtils.deserialize(rawKeys, keySerializer) :
			rawKeys;
	}


	public Boolean persist(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				return connection.persist(rawKey);
			}
		}, true);
	}


	public Boolean move(K key, final int dbIndex) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				return connection.move(rawKey, dbIndex);
			}
		}, true);
	}


	public K randomKey() {
		byte[] rawKey = execute(new RedisCallback<byte[]>() {

			public byte[] doInRedis(RedisConnection connection) {
				return connection.randomKey();
			}
		}, true);

		return deserializeKey(rawKey);
	}


	public void rename(K oldKey, K newKey) {
		final byte[] rawOldKey = rawKey(oldKey);
		final byte[] rawNewKey = rawKey(newKey);

		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) {
				connection.rename(rawOldKey, rawNewKey);
				return null;
			}
		}, true);
	}


	public Boolean renameIfAbsent(K oldKey, K newKey) {
		final byte[] rawOldKey = rawKey(oldKey);
		final byte[] rawNewKey = rawKey(newKey);

		return execute(new RedisCallback<Boolean>() {

			public Boolean doInRedis(RedisConnection connection) {
				return connection.renameNX(rawOldKey, rawNewKey);
			}
		}, true);
	}


	public DataType type(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<DataType>() {

			public DataType doInRedis(RedisConnection connection) {
				return connection.type(rawKey);
			}
		}, true);
	}

	/**
	 * Executes the Redis dump command and returns the results. Redis uses a
	 * non-standard serialization mechanism and includes checksum information,
	 * thus the raw bytes are returned as opposed to deserializing with
	 * valueSerializer. Use the return value of dump as the value argument to restore
	 *
	 * @param key The key to dump
	 * @return results The results of the dump operation
	 */
	public byte[] dump(K key) {
		final byte[] rawKey = rawKey(key);

		return execute(new RedisCallback<byte[]>() {
			public byte[] doInRedis(RedisConnection connection) {
				return connection.dump(rawKey);
			}
		}, true);
	}

	/**
	 * Executes the Redis restore command. The value passed in should be the exact
	 * serialized data returned from {@link #dump(Object)}, since Redis uses a
	 * non-standard serialization mechanism.
	 *
	 *
	 * @param key The key to restore
	 * @param value The value to restore, as returned by {@link #dump(Object)}
	 * @param timeToLive An expiration for the restored key, or 0 for no expiration
	 * @param unit The time unit for timeToLive
	 * @throws RedisSystemException if the key you are attempting to restore already
	 * exists.
	 *
	 */
	public void restore(K key, final byte[] value, long timeToLive, TimeUnit unit) {
		final byte[] rawKey = rawKey(key);
		final long rawTimeout = TimeoutUtils.toMillis(timeToLive, unit);

		execute(new RedisCallback<Object>() {
			public Boolean doInRedis(RedisConnection connection) {
				connection.restore(rawKey, rawTimeout, value);
				return null;
			}
		}, true);
	}

	public void multi() {
		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.multi();
				return null;
			}
		}, true);
	}


	public void discard() {
		execute(new RedisCallback<Object>() {


			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.discard();
				return null;
			}
		}, true);
	}


	public void watch(K key) {
		final byte[] rawKey = rawKey(key);

		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) {
				connection.watch(rawKey);
				return null;
			}
		}, true);
	}


	public void watch(Collection<K> keys) {
		final byte[][] rawKeys = rawKeys(keys);

		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) {
				connection.watch(rawKeys);
				return null;
			}
		}, true);
	}


	public void unwatch() {
		execute(new RedisCallback<Object>() {

			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.unwatch();
				return null;
			}
		}, true);
	}

	// Sort operations

	@SuppressWarnings("unchecked")
	public List<V> sort(SortQuery<K> query) {
		return sort(query, valueSerializer);
	}


	public <T> List<T> sort(SortQuery<K> query, RedisSerializer<T> resultSerializer) {
		final byte[] rawKey = rawKey(query.getKey());
		final SortParameters params = QueryUtils.convertQuery(query, stringSerializer);

		List<byte[]> vals = execute(new RedisCallback<List<byte[]>>() {

			public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sort(rawKey, params);
			}
		}, true);

		return SerializationUtils.deserialize(vals, resultSerializer);
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> sort(SortQuery<K> query, BulkMapper<T, V> bulkMapper) {
		return sort(query, bulkMapper, valueSerializer);
	}


	public <T, S> List<T> sort(SortQuery<K> query, BulkMapper<T, S> bulkMapper, RedisSerializer<S> resultSerializer) {
		List<S> values = sort(query, resultSerializer);

		if (values == null || values.isEmpty()) {
			return Collections.emptyList();
		}

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


	public Long sort(SortQuery<K> query, K storeKey) {
		final byte[] rawStoreKey = rawKey(storeKey);
		final byte[] rawKey = rawKey(query.getKey());
		final SortParameters params = QueryUtils.convertQuery(query, stringSerializer);

		return execute(new RedisCallback<Long>() {

			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sort(rawKey, params, rawStoreKey);
			}
		}, true);
	}


	public BoundValueOperations<K, V> boundValueOps(K key) {
		return new DefaultBoundValueOperations<K, V>(key, this);
	}


	public ValueOperations<K, V> opsForValue() {
		if (valueOps == null) {
			valueOps = new DefaultValueOperations<K, V>(this);
		}
		return valueOps;
	}


	public ListOperations<K, V> opsForList() {
		if (listOps == null) {
			listOps = new DefaultListOperations<K, V>(this);
		}
		return listOps;
	}


	public BoundListOperations<K, V> boundListOps(K key) {
		return new DefaultBoundListOperations<K, V>(key, this);
	}


	public BoundSetOperations<K, V> boundSetOps(K key) {
		return new DefaultBoundSetOperations<K, V>(key, this);
	}


	public SetOperations<K, V> opsForSet() {
		if (setOps == null) {
			setOps = new DefaultSetOperations<K, V>(this);
		}
		return setOps;
	}


	public BoundZSetOperations<K, V> boundZSetOps(K key) {
		return new DefaultBoundZSetOperations<K, V>(key, this);
	}


	public ZSetOperations<K, V> opsForZSet() {
		if (zSetOps == null) {
			zSetOps = new DefaultZSetOperations<K, V>(this);
		}
		return zSetOps;
	}


	public <HK, HV> BoundHashOperations<K, HK, HV> boundHashOps(K key) {
		return new DefaultBoundHashOperations<K, HK, HV>(key, this);
	}


	public <HK, HV> HashOperations<K, HK, HV> opsForHash() {
		return new DefaultHashOperations<K, HK, HV>(this);
	}
}