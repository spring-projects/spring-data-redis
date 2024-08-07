/*
 * Copyright 2015-2024 the original author or authors.
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.PartialUpdate.PropertyUpdate;
import org.springframework.data.redis.core.PartialUpdate.UpdateCommand;
import org.springframework.data.redis.core.RedisKeyValueAdapter.RedisUpdateObject.Index;
import org.springframework.data.redis.core.convert.GeoIndexedPropertyValue;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.MappingRedisConverter.BinaryKeyspaceIdentifier;
import org.springframework.data.redis.core.convert.MappingRedisConverter.KeyspaceIdentifier;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.core.convert.ReferenceResolverImpl;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.core.mapping.RedisPersistentProperty;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
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
 * @author Andrey Muchnik
 * @author John Blum
 * @since 1.7
 */
public class RedisKeyValueAdapter extends AbstractKeyValueAdapter
		implements InitializingBean, SmartLifecycle, ApplicationContextAware, ApplicationListener<RedisKeyspaceEvent> {

	/**
	 * Time To Live in seconds that phantom keys should live longer than the actual key.
	 */
	private static final int PHANTOM_KEY_TTL = 300;

	private final Log logger = LogFactory.getLog(getClass());
	private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

	private RedisOperations<?, ?> redisOps;
	private RedisConverter converter;
	private @Nullable RedisMessageListenerContainer messageListenerContainer;
	private boolean managedListenerContainer = true;
	private final AtomicReference<KeyExpirationEventMessageListener> expirationListener = new AtomicReference<>(null);
	private @Nullable ApplicationEventPublisher eventPublisher;

	private EnableKeyspaceEvents enableKeyspaceEvents = EnableKeyspaceEvents.OFF;
	private @Nullable String keyspaceNotificationsConfigParameter = null;
	private ShadowCopy shadowCopy = ShadowCopy.DEFAULT;

	/**
	 * Lifecycle state of this factory.
	 */
	enum State {
		CREATED, STARTING, STARTED, STOPPING, STOPPED, DESTROYED;
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter} with default {@link RedisMappingContext} and default
	 * {@link RedisCustomConversions}.
	 *
	 * @param redisOps must not be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps) {
		this(redisOps, new RedisMappingContext());
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter} with default {@link RedisCustomConversions}.
	 *
	 * @param redisOps must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps, RedisMappingContext mappingContext) {
		this(redisOps, mappingContext, new RedisCustomConversions());
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter}.
	 *
	 * @param redisOps must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @param customConversions can be {@literal null}.
	 * @since 2.0
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps, RedisMappingContext mappingContext,
			@Nullable org.springframework.data.convert.CustomConversions customConversions) {

		super(new RedisQueryEngine());

		Assert.notNull(redisOps, "RedisOperations must not be null");
		Assert.notNull(mappingContext, "RedisMappingContext must not be null");

		MappingRedisConverter mappingConverter = new MappingRedisConverter(mappingContext,
				new PathIndexResolver(mappingContext), new ReferenceResolverImpl(redisOps));

		mappingConverter.setCustomConversions(customConversions == null ? new RedisCustomConversions() : customConversions);
		mappingConverter.afterPropertiesSet();

		this.converter = mappingConverter;
		this.redisOps = redisOps;
		initMessageListenerContainer();
	}

	/**
	 * Creates new {@link RedisKeyValueAdapter} with specific {@link RedisConverter}.
	 *
	 * @param redisOps must not be {@literal null}.
	 * @param redisConverter must not be {@literal null}.
	 */
	public RedisKeyValueAdapter(RedisOperations<?, ?> redisOps, RedisConverter redisConverter) {

		super(new RedisQueryEngine());

		Assert.notNull(redisOps, "RedisOperations must not be null");

		this.converter = redisConverter;
		this.redisOps = redisOps;
	}

	/**
	 * Default constructor.
	 */
	protected RedisKeyValueAdapter() {}

	@Override
	public Object put(Object id, Object item, String keyspace) {

		RedisData rdo = item instanceof RedisData ? (RedisData) item : new RedisData();

		if (!(item instanceof RedisData)) {
			converter.write(item, rdo);
		}

		if (ObjectUtils.nullSafeEquals(EnableKeyspaceEvents.ON_DEMAND, enableKeyspaceEvents)
				&& this.expirationListener.get() == null) {

			if (rdo.getTimeToLive() != null && rdo.getTimeToLive() > 0) {
				initKeyExpirationListener(this.messageListenerContainer);
			}
		}

		if (rdo.getId() == null) {
			rdo.setId(converter.getConversionService().convert(id, String.class));
		}

		redisOps.execute((RedisCallback<Object>) connection -> {

			byte[] key = toBytes(rdo.getId());
			byte[] objectKey = createKey(rdo.getKeyspace(), rdo.getId());

			boolean isNew = connection.del(objectKey) == 0;

			connection.hMSet(objectKey, rdo.getBucket().rawMap());

			if (isNew) {
				connection.sAdd(toBytes(rdo.getKeyspace()), key);
			}

			if (expires(rdo)) {
				connection.expire(objectKey, rdo.getTimeToLive());
			}

			if (keepShadowCopy()) { // add phantom key so values can be restored

				byte[] phantomKey = ByteUtils.concat(objectKey, BinaryKeyspaceIdentifier.PHANTOM_SUFFIX);

				if (expires(rdo)) {
					connection.del(phantomKey);
					connection.hMSet(phantomKey, rdo.getBucket().rawMap());
					connection.expire(phantomKey, rdo.getTimeToLive() + PHANTOM_KEY_TTL);
				} else if (!isNew) {
					connection.del(phantomKey);
				}
			}

			IndexWriter indexWriter = new IndexWriter(connection, converter);

			if (isNew) {
				indexWriter.createIndexes(key, rdo.getIndexedData());
			} else {
				indexWriter.deleteAndUpdateIndexes(key, rdo.getIndexedData());
			}

			return null;
		});

		return item;
	}

	@Override
	public boolean contains(Object id, String keyspace) {

		RedisCallback<Boolean> command = connection -> connection.sIsMember(toBytes(keyspace), toBytes(id));

		return Boolean.TRUE.equals(this.redisOps.execute(command));
	}

	@Nullable
	@Override
	public Object get(Object id, String keyspace) {
		return get(id, keyspace, Object.class);
	}

	@Nullable
	@Override
	public <T> T get(Object id, String keyspace, Class<T> type) {

		String stringId = toString(id);
		byte[] binId = createKey(keyspace, stringId);

		RedisCallback<Map<byte[], byte[]>> command = connection -> connection.hGetAll(binId);

		Map<byte[], byte[]> raw = redisOps.execute(command);

		if (CollectionUtils.isEmpty(raw)) {
			return null;
		}

		RedisData data = new RedisData(raw);

		data.setId(stringId);
		data.setKeyspace(keyspace);

		return readBackTimeToLiveIfSet(binId, converter.read(type, data));
	}

	@Override
	public Object delete(Object id, String keyspace) {
		return delete(id, keyspace, Object.class);
	}

	@Override
	public <T> T delete(Object id, String keyspace, Class<T> type) {

		byte[] binId = toBytes(id);
		byte[] binKeyspace = toBytes(keyspace);

		T value = get(id, keyspace, type);

		if (value != null) {

			byte[] keyToDelete = createKey(keyspace, toString(id));

			redisOps.execute((RedisCallback<Void>) connection -> {

				connection.del(keyToDelete);
				connection.sRem(binKeyspace, binId);
				new IndexWriter(connection, converter).removeKeyFromIndexes(keyspace, binId);

				if (RedisKeyValueAdapter.this.keepShadowCopy()) {

					RedisPersistentEntity<?> persistentEntity = converter.getMappingContext().getPersistentEntity(type);

					if (persistentEntity != null && persistentEntity.isExpiring()) {

						byte[] phantomKey = ByteUtils.concat(keyToDelete, BinaryKeyspaceIdentifier.PHANTOM_SUFFIX);

						connection.del(phantomKey);
					}
				}
				return null;
			});
		}

		return value;
	}

	@Override
	public List<?> getAllOf(String keyspace) {
		return getAllOf(keyspace, Object.class, -1, -1);
	}

	@Override
	public <T> Iterable<T> getAllOf(String keyspace, Class<T> type) {
		return getAllOf(keyspace, type, -1, -1);
	}

	/**
	 * Get all elements for given keyspace.
	 *
	 * @param keyspace the keyspace to fetch entities from.
	 * @param type the desired target type.
	 * @param offset index value to start reading.
	 * @param rows maximum number or entities to return.
	 * @return never {@literal null}.
	 * @since 2.5
	 */
	public <T> List<T> getAllOf(String keyspace, Class<T> type, long offset, int rows) {

		byte[] binKeyspace = toBytes(keyspace);

		Set<byte[]> ids = redisOps.execute((RedisCallback<Set<byte[]>>) connection -> connection.sMembers(binKeyspace));

		List<T> result = new ArrayList<>();
		List<byte[]> keys = new ArrayList<>(ids);

		if (keys.isEmpty() || keys.size() < offset) {
			return Collections.emptyList();
		}

		offset = Math.max(0, offset);

		if (rows > 0) {
			keys = keys.subList((int) offset, Math.min((int) offset + rows, keys.size()));
		}

		for (byte[] key : keys) {
			result.add(get(key, keyspace, type));
		}
		return result;
	}

	@Override
	public void deleteAllOf(String keyspace) {

		redisOps.execute((RedisCallback<Void>) connection -> {

			connection.del(toBytes(keyspace));
			new IndexWriter(connection, converter).removeAllIndexes(keyspace);

			return null;
		});
	}

	@Override
	public CloseableIterator<Entry<Object, Object>> entries(String keyspace) {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	@Override
	public long count(String keyspace) {

		Long count = redisOps.execute((RedisCallback<Long>) connection -> connection.sCard(toBytes(keyspace)));

		return count != null ? count : 0;
	}

	public void update(PartialUpdate<?> update) {

		RedisPersistentEntity<?> entity = this.converter.getMappingContext()
				.getRequiredPersistentEntity(update.getTarget());

		String keyspace = entity.getKeySpace();
		Object id = update.getId();

		byte[] redisKey = createKey(keyspace, converter.getConversionService().convert(id, String.class));

		RedisData rdo = new RedisData();
		this.converter.write(update, rdo);

		redisOps.execute((RedisCallback<Void>) connection -> {

			RedisUpdateObject redisUpdateObject = new RedisUpdateObject(redisKey, keyspace, id);

			for (PropertyUpdate pUpdate : update.getPropertyUpdates()) {

				String propertyPath = pUpdate.getPropertyPath();

				if (UpdateCommand.DEL.equals(pUpdate.getCmd()) || pUpdate.getValue() instanceof Collection
						|| pUpdate.getValue() instanceof Map
						|| (pUpdate.getValue() != null && pUpdate.getValue().getClass().isArray()) || (pUpdate.getValue() != null
								&& !converter.getConversionService().canConvert(pUpdate.getValue().getClass(), byte[].class))) {

					redisUpdateObject = fetchDeletePathsFromHashAndUpdateIndex(redisUpdateObject, propertyPath, connection);
				}
			}

			if (!redisUpdateObject.fieldsToRemove.isEmpty()) {
				connection.hDel(redisKey,
						redisUpdateObject.fieldsToRemove.toArray(new byte[redisUpdateObject.fieldsToRemove.size()][]));
			}

			for (Index index : redisUpdateObject.indexesToUpdate) {

				if (ObjectUtils.nullSafeEquals(DataType.ZSET, index.type)) {
					connection.zRem(index.key, toBytes(redisUpdateObject.targetId));
				} else {
					connection.sRem(index.key, toBytes(redisUpdateObject.targetId));
				}
			}

			if (!rdo.getBucket().isEmpty()) {
				if (rdo.getBucket().size() > 1
						|| (rdo.getBucket().size() == 1 && !rdo.getBucket().asMap().containsKey("_class"))) {
					connection.hMSet(redisKey, rdo.getBucket().rawMap());
				}
			}

			if (update.isRefreshTtl()) {

				if (expires(rdo)) {

					connection.expire(redisKey, rdo.getTimeToLive());

					if (keepShadowCopy()) { // add phantom key so values can be restored

						byte[] phantomKey = ByteUtils.concat(redisKey, BinaryKeyspaceIdentifier.PHANTOM_SUFFIX);

						connection.hMSet(phantomKey, rdo.getBucket().rawMap());
						connection.expire(phantomKey, rdo.getTimeToLive() + PHANTOM_KEY_TTL);
					}

				} else {

					connection.persist(redisKey);

					if (keepShadowCopy()) {
						connection.del(ByteUtils.concat(redisKey, BinaryKeyspaceIdentifier.PHANTOM_SUFFIX));
					}
				}
			}

			new IndexWriter(connection, converter).updateIndexes(toBytes(id), rdo.getIndexedData());
			return null;
		});
	}

	private RedisUpdateObject fetchDeletePathsFromHashAndUpdateIndex(RedisUpdateObject redisUpdateObject, String path,
			RedisConnection connection) {

		redisUpdateObject.addFieldToRemove(toBytes(path));

		byte[] value = connection.hGet(redisUpdateObject.targetKey, toBytes(path));

		if (value != null && value.length > 0) {

			byte[] existingValueIndexKey = ByteUtils.concatAll(toBytes(redisUpdateObject.keyspace), toBytes(":" + path),
					toBytes(":"), value);

			if (connection.exists(existingValueIndexKey)) {
				redisUpdateObject.addIndexToUpdate(new RedisUpdateObject.Index(existingValueIndexKey, DataType.SET));
			}

			return redisUpdateObject;
		}

		Set<byte[]> existingFields = connection.hKeys(redisUpdateObject.targetKey);

		for (byte[] field : existingFields) {

			if (toString(field).startsWith(path + ".")) {

				redisUpdateObject.addFieldToRemove(field);
				value = connection.hGet(redisUpdateObject.targetKey, toBytes(field));

				if (value != null) {

					byte[] existingValueIndexKey = ByteUtils.concatAll(toBytes(redisUpdateObject.keyspace), toBytes(":"), field,
							toBytes(":"), value);

					if (connection.exists(existingValueIndexKey)) {
						redisUpdateObject.addIndexToUpdate(new RedisUpdateObject.Index(existingValueIndexKey, DataType.SET));
					}
				}
			}
		}

		String pathToUse = GeoIndexedPropertyValue.geoIndexName(path);
		byte[] existingGeoIndexKey = ByteUtils.concatAll(toBytes(redisUpdateObject.keyspace), toBytes(":"),
				toBytes(pathToUse));

		if (connection.zRank(existingGeoIndexKey, toBytes(redisUpdateObject.targetId)) != null) {
			redisUpdateObject.addIndexToUpdate(new RedisUpdateObject.Index(existingGeoIndexKey, DataType.ZSET));
		}

		return redisUpdateObject;
	}

	/**
	 * Execute {@link RedisCallback} via underlying {@link RedisOperations}.
	 *
	 * @param callback must not be {@literal null}.
	 * @see RedisOperations#execute(RedisCallback)
	 */
	@Nullable
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

	/**
	 * Creates a new {@link byte[] key} using the given {@link String keyspace} and {@link String id}.
	 *
	 * @param keyspace {@link String name} of the Redis {@literal keyspace}.
	 * @param id {@link String} identifying the key.
	 * @return a {@link byte[]} constructed from the {@link String keyspace} and {@link String id}.
	 */
	public byte[] createKey(String keyspace, String id) {
		return toBytes(keyspace + ":" + id);
	}

	/**
	 * Convert given source to binary representation using the underlying {@link ConversionService}.
	 */
	public byte[] toBytes(Object source) {
		return source instanceof byte[] bytes ? bytes : getConverter().getConversionService().convert(source, byte[].class);
	}

	private String toString(Object value) {
		return value instanceof String stringValue ? stringValue
				: getConverter().getConversionService().convert(value, String.class);
	}

	/**
	 * Read back and set {@link TimeToLive} for the property.
	 */
	@Nullable
	private <T> T readBackTimeToLiveIfSet(@Nullable byte[] key, @Nullable T target) {

		if (target == null || key == null) {
			return target;
		}

		RedisPersistentEntity<?> entity = this.converter.getMappingContext().getRequiredPersistentEntity(target.getClass());

		if (entity.hasExplicitTimeToLiveProperty()) {

			RedisPersistentProperty ttlProperty = entity.getExplicitTimeToLiveProperty();

			if (ttlProperty == null) {
				return target;
			}

			TimeToLive ttl = ttlProperty.findAnnotation(TimeToLive.class);

			Long timeout = redisOps.execute((RedisCallback<Long>) connection -> {

				if (ObjectUtils.nullSafeEquals(TimeUnit.SECONDS, ttl.unit())) {
					return connection.ttl(key);
				}

				return connection.pTtl(key, ttl.unit());
			});

			if (timeout != null || !ttlProperty.getType().isPrimitive()) {

				PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(target);

				propertyAccessor.setProperty(ttlProperty,
						converter.getConversionService().convert(timeout, ttlProperty.getType()));

				target = propertyAccessor.getBean();
			}
		}

		return target;
	}

	/**
	 * @return {@literal true} if {@link RedisData#getTimeToLive()} has a positive value.
	 * @param data must not be {@literal null}.
	 * @since 2.3.7
	 */
	private boolean expires(RedisData data) {
		return data.getTimeToLive() != null && data.getTimeToLive() > 0;
	}

	/**
	 * Configure usage of {@link KeyExpirationEventMessageListener}.
	 *
	 * @since 1.8
	 */
	public void setEnableKeyspaceEvents(EnableKeyspaceEvents enableKeyspaceEvents) {
		this.enableKeyspaceEvents = enableKeyspaceEvents;
	}

	/**
	 * Configure a {@link RedisMessageListenerContainer} to listen for Keyspace expiry events. The container can only be
	 * set when this bean hasn't been yet {@link #afterPropertiesSet() initialized}.
	 *
	 * @param messageListenerContainer the container to use.
	 * @since 2.7.2
	 * @throws IllegalStateException when trying to set a {@link RedisMessageListenerContainer} after
	 *           {@link #afterPropertiesSet()} has been called to initialize a managed container instance.
	 */
	public void setMessageListenerContainer(RedisMessageListenerContainer messageListenerContainer) {

		Assert.notNull(messageListenerContainer, "RedisMessageListenerContainer must not be null");

		if (this.managedListenerContainer && this.messageListenerContainer != null) {
			throw new IllegalStateException(
					"Cannot set RedisMessageListenerContainer after initializing a managed RedisMessageListenerContainer instance");
		}

		this.managedListenerContainer = false;
		this.messageListenerContainer = messageListenerContainer;
	}

	/**
	 * Configure the {@literal notify-keyspace-events} property if not already set. Use an empty {@link String} or
	 * {@literal null} to retain existing server settings.
	 *
	 * @param keyspaceNotificationsConfigParameter can be {@literal null}.
	 * @since 1.8
	 */
	public void setKeyspaceNotificationsConfigParameter(String keyspaceNotificationsConfigParameter) {
		this.keyspaceNotificationsConfigParameter = keyspaceNotificationsConfigParameter;
	}

	/**
	 * Configure storage of phantom keys (shadow copies) of expiring entities.
	 *
	 * @param shadowCopy must not be {@literal null}.
	 * @since 2.3
	 */
	public void setShadowCopy(ShadowCopy shadowCopy) {
		this.shadowCopy = shadowCopy;
	}

	@Override
	public boolean isRunning() {
		return State.STARTED.equals(this.state.get());
	}

	/**
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 * @since 1.8
	 */
	@Override
	public void afterPropertiesSet() {

		if (this.managedListenerContainer) {
			initMessageListenerContainer();
		}
	}

	@Override
	public void start() {

		State current = this.state.getAndUpdate(state -> isCreatedOrStopped(state) ? State.STARTING : state);

		if (isCreatedOrStopped(current)) {

			messageListenerContainer.start();

			if (ObjectUtils.nullSafeEquals(EnableKeyspaceEvents.ON_STARTUP, this.enableKeyspaceEvents)) {
				initKeyExpirationListener(this.messageListenerContainer);
			}

			this.state.set(State.STARTED);
		}
	}

	private static boolean isCreatedOrStopped(@Nullable State state) {
		return State.CREATED.equals(state) || State.STOPPED.equals(state);
	}

	@Override
	public void stop() {

		if (state.compareAndSet(State.STARTED, State.STOPPING)) {

			KeyExpirationEventMessageListener listener = this.expirationListener.get();
			if (listener != null) {

				if (this.expirationListener.compareAndSet(listener, null)) {
					try {
						listener.destroy();
					} catch (Exception e) {
						logger.warn("Could not destroy KeyExpirationEventMessageListener", e);
					}
				}
			}

			messageListenerContainer.stop();
			state.set(State.STOPPED);
		}
	}

	public void destroy() throws Exception {

		stop();

		if (this.managedListenerContainer && this.messageListenerContainer != null) {
			this.messageListenerContainer.destroy();
			this.messageListenerContainer = null;
		}

		this.state.set(State.DESTROYED);
	}

	@Override
	public void onApplicationEvent(RedisKeyspaceEvent event) {
		// just a customization hook
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.eventPublisher = applicationContext;
	}

	private void initMessageListenerContainer() {

		this.messageListenerContainer = new RedisMessageListenerContainer();
		this.messageListenerContainer.setConnectionFactory(((RedisTemplate<?, ?>) redisOps).getConnectionFactory());
		this.messageListenerContainer.afterPropertiesSet();
	}

	private void initKeyExpirationListener(RedisMessageListenerContainer messageListenerContainer) {

		if (this.expirationListener.get() == null) {
			MappingExpirationListener listener = new MappingExpirationListener(messageListenerContainer, this.redisOps,
					this.converter, this.shadowCopy);

			listener.setKeyspaceNotificationsConfigParameter(keyspaceNotificationsConfigParameter);

			if (this.eventPublisher != null) {
				listener.setApplicationEventPublisher(this.eventPublisher);
			}

			if (this.expirationListener.compareAndSet(null, listener)) {
				listener.init();
			}
		}
	}

	/**
	 * {@link MessageListener} implementation used to capture Redis keyspace notifications. Tries to read a previously
	 * created phantom key {@code keyspace:id:phantom} to provide the expired object as part of the published
	 * {@link RedisKeyExpiredEvent}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class MappingExpirationListener extends KeyExpirationEventMessageListener {

		private final RedisOperations<?, ?> ops;
		private final RedisConverter converter;
		private final ShadowCopy shadowCopy;

		/**
		 * Creates new {@link MappingExpirationListener}.
		 */
		MappingExpirationListener(RedisMessageListenerContainer listenerContainer, RedisOperations<?, ?> ops,
				RedisConverter converter, ShadowCopy shadowCopy) {

			super(listenerContainer);
			this.ops = ops;
			this.converter = converter;
			this.shadowCopy = shadowCopy;
		}

		@Override
		public void onMessage(Message message, @Nullable byte[] pattern) {

			if (!isKeyExpirationMessage(message)) {
				return;
			}

			byte[] key = message.getBody();
			Object value = readShadowCopyIfEnabled(key);
			byte[] channelAsBytes = message.getChannel();

			String channel = !ObjectUtils.isEmpty(channelAsBytes)
					? converter.getConversionService().convert(channelAsBytes, String.class)
					: null;

			RedisKeyExpiredEvent<?> event = new RedisKeyExpiredEvent<>(channel, key, value);

			ops.execute((RedisCallback<Void>) connection -> {

				connection.sRem(converter.getConversionService().convert(event.getKeyspace(), byte[].class), event.getId());
				new IndexWriter(connection, converter).removeKeyFromIndexes(event.getKeyspace(), event.getId());
				return null;
			});

			publishEvent(event);
		}

		private boolean isKeyExpirationMessage(Message message) {
			return BinaryKeyspaceIdentifier.isValid(message.getBody());
		}

		@Nullable
		private Object readShadowCopyIfEnabled(byte[] key) {

			if (shadowCopy == ShadowCopy.OFF) {
				return null;
			}
			return readShadowCopy(key);
		}

		@Nullable
		private Object readShadowCopy(byte[] key) {

			byte[] phantomKey = ByteUtils.concat(key,
					converter.getConversionService().convert(KeyspaceIdentifier.PHANTOM_SUFFIX, byte[].class));

			Map<byte[], byte[]> hash = ops.execute((RedisCallback<Map<byte[], byte[]>>) connection -> {

				Map<byte[], byte[]> phantomValue = connection.hGetAll(phantomKey);

				if (!CollectionUtils.isEmpty(phantomValue)) {
					connection.del(phantomKey);
				}

				return phantomValue;
			});

			return CollectionUtils.isEmpty(hash) ? null : converter.read(Object.class, new RedisData(hash));
		}
	}

	private boolean keepShadowCopy() {

		return switch (shadowCopy) {
			case OFF -> false;
			case ON -> true;
			default -> this.expirationListener.get() != null;
		};
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	public enum EnableKeyspaceEvents {

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

	/**
	 * Configuration flag controlling storage of phantom keys (shadow copies) of expiring entities to read them later when
	 * publishing {@link RedisKeyspaceEvent}.
	 *
	 * @author Christoph Strobl
	 * @since 2.4
	 */
	public enum ShadowCopy {

		/**
		 * Store shadow copies of expiring entities depending on the {@link EnableKeyspaceEvents}.
		 */
		DEFAULT,

		/**
		 * Store shadow copies of expiring entities.
		 */
		ON,

		/**
		 * Do not store shadow copies.
		 */
		OFF
	}

	/**
	 * Container holding update information like fields to remove from the Redis Hash.
	 *
	 * @author Christoph Strobl
	 */
	static class RedisUpdateObject {

		private final String keyspace;
		private final Object targetId;
		private final byte[] targetKey;

		private final Set<byte[]> fieldsToRemove = new LinkedHashSet<>();
		private final Set<Index> indexesToUpdate = new LinkedHashSet<>();

		RedisUpdateObject(byte[] targetKey, String keyspace, Object targetId) {

			this.targetKey = targetKey;
			this.keyspace = keyspace;
			this.targetId = targetId;
		}

		void addFieldToRemove(byte[] field) {
			fieldsToRemove.add(field);
		}

		void addIndexToUpdate(Index index) {
			indexesToUpdate.add(index);
		}

		static class Index {

			final DataType type;
			final byte[] key;

			public Index(byte[] key, DataType type) {
				this.key = key;
				this.type = type;
			}
		}
	}
}
