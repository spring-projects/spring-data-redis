/*
 * Copyright 2015-2016 the original author or authors.
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.convert.CustomConversions;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.core.convert.ReferenceResolverImpl;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Redis specific {@link KeyValueAdapter} implementation. Uses binary codec to read/write data from/to Redis. Objects
 * are stored in a Redis Hash using the value of {@link RedisHash}, the {@link KeyspaceConfiguration} or just
 * {@link Class#getName()} as a prefix. <br />
 * <strong>Example</strong>
 *
 * <pre>
 * <code>
 * &#64;RedisHash("persons")
 * class Person {
 *   &#64;Id String id;
 *   String name;
 * }
 *
 *
 *         prefix              ID
 *           |                 |
 *           V                 V
 * hgetall persons:5d67b7e1-8640-4475-beeb-c666fab4c0e5
 * 1) id
 * 2) 5d67b7e1-8640-4475-beeb-c666fab4c0e5
 * 3) name
 * 4) Rand al'Thor
 * </code>
 * </pre>
 *
 * <br />
 * The {@link KeyValueAdapter} is <strong>not</strong> intended to store simple types such as {@link String} values.
 * Please use {@link RedisTemplate} for this purpose.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisKeyValueAdapter extends AbstractKeyValueAdapter
		implements InitializingBean, ApplicationContextAware, ApplicationListener<RedisKeyspaceEvent> {

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisKeyValueAdapter.class);

	private RedisOperations<?, ?> redisOps;
	private RedisConverter converter;
	private RedisMessageListenerContainer messageListenerContainer;
	private final AtomicReference<KeyExpirationEventMessageListener> expirationListener = new AtomicReference<KeyExpirationEventMessageListener>(
			null);
	private ApplicationEventPublisher eventPublisher;

	private EnableKeyspaceEvents enableKeyspaceEvents = EnableKeyspaceEvents.OFF;

	/**
	 * Creates new {@link RedisKeyValueAdapter} with default {@link RedisMappingContext} and default
	 * {@link CustomConversions}.
	 *
	 * @param redisOps must not be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps) {
		this(redisOps, new RedisMappingContext());
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter} with default {@link CustomConversions}.
	 *
	 * @param redisOps must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps, RedisMappingContext mappingContext) {
		this(redisOps, mappingContext, new CustomConversions());
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter}.
	 *
	 * @param redisOps must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @param customConversions can be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps, RedisMappingContext mappingContext,
			CustomConversions customConversions) {

		super(new RedisQueryEngine());

		Assert.notNull(redisOps, "RedisOperations must not be null!");
		Assert.notNull(mappingContext, "RedisMappingContext must not be null!");

		MappingRedisConverter mappingConverter = new MappingRedisConverter(mappingContext,
				new PathIndexResolver(mappingContext), new ReferenceResolverImpl(redisOps));
		mappingConverter.setCustomConversions(customConversions == null ? new CustomConversions() : customConversions);
		mappingConverter.afterPropertiesSet();

		this.converter = mappingConverter;
		this.redisOps = redisOps;
		initMessageListenerContainer();
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter} with specific {@link RedisConverter}.
	 *
	 * @param redisOps must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps, RedisConverter redisConverter) {

		super(new RedisQueryEngine());

		Assert.notNull(redisOps, "RedisOperations must not be null!");

		this.converter = redisConverter;
		this.redisOps = redisOps;
		initMessageListenerContainer();
	}

	/**
	 * Default constructor.
	 */
	protected RedisKeyValueAdapter() {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#put(java.io.Serializable, java.lang.Object, java.io.Serializable)
	 */
	public Object put(final Serializable id, final Object item, final Serializable keyspace) {

		final RedisData rdo = item instanceof RedisData ? (RedisData) item : new RedisData();
		if (!(item instanceof RedisData)) {
			converter.write(item, rdo);
		}

		if (ObjectUtils.nullSafeEquals(EnableKeyspaceEvents.ON_DEMAND, enableKeyspaceEvents)
				&& this.expirationListener.get() == null) {

			if (rdo.getTimeToLive() != null && rdo.getTimeToLive().longValue() > 0) {
				initKeyExpirationListener();
			}
		}

		if (rdo.getId() == null) {

			rdo.setId(converter.getConversionService().convert(id, String.class));

			if (!(item instanceof RedisData)) {
				KeyValuePersistentProperty idProperty = converter.getMappingContext().getPersistentEntity(item.getClass())
						.getIdProperty();
				converter.getMappingContext().getPersistentEntity(item.getClass()).getPropertyAccessor(item)
						.setProperty(idProperty, id);
			}
		}

		redisOps.execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws DataAccessException {

				byte[] key = toBytes(rdo.getId());
				byte[] objectKey = createKey(rdo.getKeyspace(), rdo.getId());

				boolean isNew = connection.del(objectKey) == 0;

				connection.hMSet(objectKey, rdo.getBucket().rawMap());

				if (rdo.getTimeToLive() != null && rdo.getTimeToLive().longValue() > 0) {

					connection.expire(objectKey, rdo.getTimeToLive().longValue());

					// add phantom key so values can be restored
					byte[] phantomKey = ByteUtils.concat(objectKey, toBytes(":phantom"));
					connection.del(phantomKey);
					connection.hMSet(phantomKey, rdo.getBucket().rawMap());
					connection.expire(phantomKey, rdo.getTimeToLive().longValue() + 300);
				}

				connection.sAdd(toBytes(rdo.getKeyspace()), key);

				IndexWriter indexWriter = new IndexWriter(connection, converter);
				if (isNew) {
					indexWriter.createIndexes(key, rdo.getIndexedData());
				} else {
					indexWriter.updateIndexes(key, rdo.getIndexedData());
				}
				return null;
			}
		});

		return item;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#contains(java.io.Serializable, java.io.Serializable)
	 */
	public boolean contains(final Serializable id, final Serializable keyspace) {

		Boolean exists = redisOps.execute(new RedisCallback<Boolean>() {

			@Override
			public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sIsMember(toBytes(keyspace), toBytes(id));
			}
		});

		return exists != null ? exists.booleanValue() : false;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#get(java.io.Serializable, java.io.Serializable)
	 */
	public Object get(Serializable id, Serializable keyspace) {
		return get(id, keyspace, Object.class);
	}

	/**
	 * @param id
	 * @param keyspace
	 * @param type
	 * @return
	 */
	public <T> T get(Serializable id, Serializable keyspace, Class<T> type) {

		String stringId = asString(id);
		String stringKeyspace = asString(keyspace);

		final byte[] binId = createKey(stringKeyspace, stringId);

		Map<byte[], byte[]> raw = redisOps.execute(new RedisCallback<Map<byte[], byte[]>>() {

			@Override
			public Map<byte[], byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.hGetAll(binId);
			}
		});

		RedisData data = new RedisData(raw);
		data.setId(stringId);
		data.setKeyspace(stringKeyspace);

		return converter.read(type, data);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#delete(java.io.Serializable, java.io.Serializable)
	 */
	public Object delete(final Serializable id, final Serializable keyspace) {
		return delete(id, keyspace, Object.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.AbstractKeyValueAdapter#delete(java.io.Serializable, java.io.Serializable, java.lang.Class)
	 */
	public <T> T delete(final Serializable id, final Serializable keyspace, final Class<T> type) {

		final byte[] binId = toBytes(id);
		final byte[] binKeyspace = toBytes(keyspace);

		T o = get(id, keyspace, type);

		if (o != null) {

			final byte[] keyToDelete = createKey(asString(keyspace), asString(id));

			redisOps.execute(new RedisCallback<Void>() {

				@Override
				public Void doInRedis(RedisConnection connection) throws DataAccessException {

					connection.del(keyToDelete);
					connection.sRem(binKeyspace, binId);

					new IndexWriter(connection, converter).removeKeyFromIndexes(asString(keyspace), binId);
					return null;
				}
			});

		}
		return o;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#getAllOf(java.io.Serializable)
	 */
	public List<?> getAllOf(final Serializable keyspace) {

		final byte[] binKeyspace = toBytes(keyspace);

		List<Map<byte[], byte[]>> raw = redisOps.execute(new RedisCallback<List<Map<byte[], byte[]>>>() {

			@Override
			public List<Map<byte[], byte[]>> doInRedis(RedisConnection connection) throws DataAccessException {

				final List<Map<byte[], byte[]>> rawData = new ArrayList<Map<byte[], byte[]>>();

				Set<byte[]> members = connection.sMembers(binKeyspace);

				for (byte[] id : members) {
					rawData.add(connection
							.hGetAll(createKey(asString(keyspace), getConverter().getConversionService().convert(id, String.class))));
				}

				return rawData;
			}
		});

		List<Object> result = new ArrayList<Object>(raw.size());
		for (Map<byte[], byte[]> rawData : raw) {
			result.add(converter.read(Object.class, new RedisData(rawData)));
		}

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#deleteAllOf(java.io.Serializable)
	 */
	public void deleteAllOf(final Serializable keyspace) {

		redisOps.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				connection.del(toBytes(keyspace));
				new IndexWriter(connection, converter).removeAllIndexes(asString(keyspace));
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#entries(java.io.Serializable)
	 */
	public CloseableIterator<Entry<Serializable, Object>> entries(Serializable keyspace) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#count(java.io.Serializable)
	 */
	public long count(final Serializable keyspace) {

		Long count = redisOps.execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.sCard(toBytes(keyspace));
			}
		});

		return count != null ? count.longValue() : 0;
	}

	/**
	 * Execute {@link RedisCallback} via underlying {@link RedisOperations}.
	 *
	 * @param callback must not be {@literal null}.
	 * @see RedisOperations#execute(RedisCallback)
	 * @return
	 */
	public <T> T execute(RedisCallback<T> callback) {
		return redisOps.execute(callback);
	}

	/**
	 * Get the {@link RedisConverter} in use.
	 *
	 * @return never {@literal null}.
	 */
	public RedisConverter getConverter() {
		return this.converter;
	}

	public void clear() {
		// nothing to do
	}

	private String asString(Serializable value) {
		return value instanceof String ? (String) value
				: getConverter().getConversionService().convert(value, String.class);
	}

	public byte[] createKey(String keyspace, String id) {
		return toBytes(keyspace + ":" + id);
	}

	/**
	 * Convert given source to binary representation using the underlying {@link ConversionService}.
	 *
	 * @param source
	 * @return
	 * @throws ConverterNotFoundException
	 */
	public byte[] toBytes(Object source) {

		if (source instanceof byte[]) {
			return (byte[]) source;
		}

		return converter.getConversionService().convert(source, byte[].class);
	}

	/**
	 * Configure usage of {@link KeyExpirationEventMessageListener}.
	 *
	 * @param enableKeyspaceEvents
	 * @since 1.8
	 */
	public void setEnableKeyspaceEvents(EnableKeyspaceEvents enableKeyspaceEvents) {
		this.enableKeyspaceEvents = enableKeyspaceEvents;
	}

	/**
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 * @since 1.8
	 */
	@Override
	public void afterPropertiesSet() {

		if (ObjectUtils.nullSafeEquals(EnableKeyspaceEvents.ON_STARTUP, this.enableKeyspaceEvents)) {
			initKeyExpirationListener();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {

		if (this.expirationListener.get() != null) {
			this.expirationListener.get().destroy();
		}

		if(this.messageListenerContainer != null){
			this.messageListenerContainer.destroy();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	@Override
	public void onApplicationEvent(RedisKeyspaceEvent event) {

		LOGGER.debug("Received %s .", event);

		if (event instanceof RedisKeyExpiredEvent) {

			final RedisKeyExpiredEvent expiredEvent = (RedisKeyExpiredEvent) event;

			redisOps.execute(new RedisCallback<Void>() {

				@Override
				public Void doInRedis(RedisConnection connection) throws DataAccessException {

					LOGGER.debug("Cleaning up expired key '%s' data structures in keyspace '%s'.", expiredEvent.getSource(),
							expiredEvent.getKeyspace());

					connection.sRem(toBytes(expiredEvent.getKeyspace()), expiredEvent.getId());
					new IndexWriter(connection, converter).removeKeyFromIndexes(expiredEvent.getKeyspace(), expiredEvent.getId());
					return null;
				}
			});

		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.eventPublisher = applicationContext;
	}

	private void initMessageListenerContainer() {

		this.messageListenerContainer = new RedisMessageListenerContainer();
		this.messageListenerContainer.setConnectionFactory(((RedisTemplate<?, ?>) redisOps).getConnectionFactory());
		this.messageListenerContainer.afterPropertiesSet();
		this.messageListenerContainer.start();
	}

	private void initKeyExpirationListener() {

		if (this.expirationListener.get() == null) {

			MappingExpirationListener listener = new MappingExpirationListener(this.messageListenerContainer, this.redisOps,
					this.converter);

			if (this.eventPublisher != null) {
				listener.setApplicationEventPublisher(this.eventPublisher);
			}

			if (this.expirationListener.compareAndSet(null, listener)) {
				listener.init();
			}
		}
	}

	/**
	 * {@link MessageListener} implementation used to capture Redis keypspace notifications. Tries to read a previously
	 * created phantom key {@code keyspace:id:phantom} to provide the expired object as part of the published
	 * {@link RedisKeyExpiredEvent}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class MappingExpirationListener extends KeyExpirationEventMessageListener {

		private final RedisOperations<?, ?> ops;
		private final RedisConverter converter;

		/**
		 * Creates new {@link MappingExpirationListener}.
		 *
		 * @param listenerContainer
		 * @param ops
		 * @param converter
		 */
		public MappingExpirationListener(RedisMessageListenerContainer listenerContainer, RedisOperations<?, ?> ops,
				RedisConverter converter) {

			super(listenerContainer);
			this.ops = ops;
			this.converter = converter;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.listener.KeyspaceEventMessageListener#onMessage(org.springframework.data.redis.connection.Message, byte[])
		 */
		@Override
		public void onMessage(Message message, byte[] pattern) {

			if (!isKeyExpirationMessage(message)) {
				return;
			}

			byte[] key = message.getBody();

			final byte[] phantomKey = ByteUtils.concat(key,
					converter.getConversionService().convert(":phantom", byte[].class));

			Map<byte[], byte[]> hash = ops.execute(new RedisCallback<Map<byte[], byte[]>>() {

				@Override
				public Map<byte[], byte[]> doInRedis(RedisConnection connection) throws DataAccessException {

					Map<byte[], byte[]> hash = connection.hGetAll(phantomKey);

					if (!org.springframework.util.CollectionUtils.isEmpty(hash)) {
						connection.del(phantomKey);
					}
					return hash;
				}
			});

			Object value = converter.read(Object.class, new RedisData(hash));
			publishEvent(new RedisKeyExpiredEvent(key, value));
		}

		private boolean isKeyExpirationMessage(Message message) {

			if (message == null || message.getChannel() == null || message.getBody() == null) {
				return false;
			}

			byte[][] args = ByteUtils.split(message.getBody(), ':');
			if (args.length != 2) {
				return false;
			}

			return true;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	public static enum EnableKeyspaceEvents {

		/**
		 * Initializes the {@link KeyExpirationEventMessageListener} on startup.
		 */
		ON_STARTUP,

		/**
		 * Initializes the {@link KeyExpirationEventMessageListener} on first insert having expiration time set.
		 */
		ON_DEMAND,

		/**
		 * Turn {@link KeyExpirationEventMessageListener} usage off. No expiration events will be received.
		 */
		OFF
	}

}
