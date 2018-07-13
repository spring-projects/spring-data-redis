/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.PartialUpdate.PropertyUpdate;
import org.springframework.data.redis.core.PartialUpdate.UpdateCommand;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.core.mapping.RedisPersistentProperty;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.comparator.NullSafeComparator;

/**
 * {@link RedisConverter} implementation creating flat binary map structure out of a given domain type. Considers
 * {@link Indexed} annotation for enabling helper structures for finder operations. <br />
 * <br />
 * <strong>NOTE</strong> {@link MappingRedisConverter} is an {@link InitializingBean} and requires
 * {@link MappingRedisConverter#afterPropertiesSet()} to be called.
 *
 * <pre>
 * <code>
 * &#64;RedisHash("persons")
 * class Person {
 *
 *   &#64;Id String id;
 *   String firstname;
 *
 *   List<String> nicknames;
 *   List<Person> coworkers;
 *
 *   Address address;
 *   &#64;Reference Country nationality;
 * }
 * </code>
 * </pre>
 *
 * The above is represented as:
 *
 * <pre>
 * <code>
 * _class=org.example.Person
 * id=1
 * firstname=rand
 * lastname=al'thor
 * coworkers.[0].firstname=mat
 * coworkers.[0].nicknames.[0]=prince of the ravens
 * coworkers.[1].firstname=perrin
 * coworkers.[1].address.city=two rivers
 * nationality=nationality:andora
 * </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Greg Turnquist
 * @author Mark Paluch
 * @since 1.7
 */
public class MappingRedisConverter implements RedisConverter, InitializingBean {

	private static final String INVALID_TYPE_ASSIGNMENT = "Value of type %s cannot be assigned to property %s of type %s.";

	private final RedisMappingContext mappingContext;
	private final GenericConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final RedisTypeMapper typeMapper;
	private final Comparator<String> listKeyComparator = new NullSafeComparator<>(NaturalOrderingKeyComparator.INSTANCE,
			true);

	private IndexResolver indexResolver;
	private @Nullable ReferenceResolver referenceResolver;
	private CustomConversions customConversions;

	/**
	 * Creates new {@link MappingRedisConverter}.
	 *
	 * @param context can be {@literal null}.
	 */
	MappingRedisConverter(RedisMappingContext context) {
		this(context, null, null, null);
	}

	/**
	 * Creates new {@link MappingRedisConverter} and defaults {@link RedisMappingContext} when {@literal null}.
	 *
	 * @param mappingContext can be {@literal null}.
	 * @param indexResolver can be {@literal null}.
	 * @param referenceResolver can be not be {@literal null}.
	 */
	public MappingRedisConverter(@Nullable RedisMappingContext mappingContext, @Nullable IndexResolver indexResolver,
			@Nullable ReferenceResolver referenceResolver) {
		this(mappingContext, indexResolver, referenceResolver, null);
	}

	/**
	 * Creates new {@link MappingRedisConverter} and defaults {@link RedisMappingContext} when {@literal null}.
	 *
	 * @param mappingContext can be {@literal null}.
	 * @param indexResolver can be {@literal null}.
	 * @param referenceResolver can be {@literal null}.
	 * @param typeMapper can be {@literal null}.
	 * @since 2.1
	 */
	public MappingRedisConverter(@Nullable RedisMappingContext mappingContext, @Nullable IndexResolver indexResolver,
			@Nullable ReferenceResolver referenceResolver, @Nullable RedisTypeMapper typeMapper) {

		this.mappingContext = mappingContext != null ? mappingContext : new RedisMappingContext();

		this.entityInstantiators = new EntityInstantiators();
		this.conversionService = new DefaultConversionService();
		this.customConversions = new RedisCustomConversions();
		this.typeMapper = typeMapper != null ? typeMapper
				: new DefaultRedisTypeMapper(DefaultRedisTypeMapper.DEFAULT_TYPE_KEY, this.mappingContext);

		this.indexResolver = indexResolver != null ? indexResolver : new PathIndexResolver(this.mappingContext);
		this.referenceResolver = referenceResolver;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityReader#read(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <R> R read(Class<R> type, RedisData source) {
		return readInternal("", type, source);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <R> R readInternal(String path, Class<R> type, RedisData source) {

		if (source.getBucket() == null || source.getBucket().isEmpty()) {
			return null;
		}

		TypeInformation<?> readType = typeMapper.readType(source.getBucket().getPath(), ClassTypeInformation.from(type));

		RedisPersistentEntity<?> entity = mappingContext.getPersistentEntity(readType);

		if (customConversions.hasCustomReadTarget(Map.class, readType.getType())) {

			Map<String, byte[]> partial = new HashMap<>();

			if (!path.isEmpty()) {

				for (Entry<String, byte[]> entry : source.getBucket().extract(path + ".").entrySet()) {
					partial.put(entry.getKey().substring(path.length() + 1), entry.getValue());
				}

			} else {
				partial.putAll(source.getBucket().asMap());
			}
			R instance = (R) conversionService.convert(partial, readType.getType());

			if (entity != null && entity.hasIdProperty()) {

				PersistentPropertyAccessor<R> propertyAccessor = entity.getPropertyAccessor(instance);

				propertyAccessor.setProperty(entity.getRequiredIdProperty(), source.getId());
				instance = propertyAccessor.getBean();
			}
			return instance;
		}

		if (conversionService.canConvert(byte[].class, readType.getType())) {
			return (R) conversionService.convert(source.getBucket().get(StringUtils.hasText(path) ? path : "_raw"),
					readType.getType());
		}

		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);

		Object instance = instantiator.createInstance((RedisPersistentEntity<RedisPersistentProperty>) entity,
				new PersistentEntityParameterValueProvider<>(entity,
						new ConverterAwareParameterValueProvider(path, source, conversionService), this.conversionService));

		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);

		entity.doWithProperties((PropertyHandler<RedisPersistentProperty>) persistentProperty -> {

			PreferredConstructor<?, RedisPersistentProperty> constructor = entity.getPersistenceConstructor();

			if (constructor.isConstructorParameter(persistentProperty)) {
				return;
			}

			Object targetValue = readProperty(path, source, persistentProperty);

			if (targetValue != null) {
				accessor.setProperty(persistentProperty, targetValue);
			}
		});

		readAssociation(path, source, entity, accessor);

		return (R) accessor.getBean();
	}

	@SuppressWarnings("unchecked")
	@Nullable
	protected Object readProperty(String path, RedisData source, RedisPersistentProperty persistentProperty) {

		String currentPath = !path.isEmpty() ? path + "." + persistentProperty.getName() : persistentProperty.getName();

		TypeInformation<?> typeInformation = persistentProperty.getTypeInformation();

		if (persistentProperty.isMap()) {

			Class<?> mapValueType = persistentProperty.getMapValueType();

			if (mapValueType == null) {
				throw new IllegalArgumentException("Unable to retrieve MapValueType!");
			}

			if (conversionService.canConvert(byte[].class, mapValueType)) {
				return readMapOfSimpleTypes(currentPath, typeInformation.getType(),
						typeInformation.getRequiredComponentType().getType(), mapValueType, source);
			}

			return readMapOfComplexTypes(currentPath, typeInformation.getType(),
					typeInformation.getRequiredComponentType().getType(), mapValueType, source);
		}

		if (typeInformation.isCollectionLike()) {

			return readCollectionOrArray(currentPath, typeInformation.getType(),
					typeInformation.getRequiredComponentType().getActualType().getType(), source.getBucket());
		}

		if (persistentProperty.isEntity()
				&& !conversionService.canConvert(byte[].class, typeInformation.getActualType().getType())) {

			Bucket bucket = source.getBucket().extract(currentPath + ".");

			RedisData newBucket = new RedisData(bucket);
			TypeInformation<?> typeToRead = typeMapper.readType(bucket.getPropertyPath(currentPath),
					typeInformation.getActualType());

			return readInternal(currentPath, typeToRead.getType(), newBucket);
		}

		byte[] sourceBytes = source.getBucket().get(currentPath);

		if (persistentProperty.isIdProperty() && StringUtils.isEmpty(path.isEmpty())) {
			return sourceBytes == null ? fromBytes(sourceBytes, typeInformation.getActualType().getType()) : source.getId();
		}

		Class<?> typeToUse = getTypeHint(currentPath, source.getBucket(), persistentProperty.getActualType());
		return fromBytes(sourceBytes, typeToUse);
	}

	private void readAssociation(String path, RedisData source, RedisPersistentEntity<?> entity,
			PersistentPropertyAccessor accessor) {

		entity.doWithAssociations((AssociationHandler<RedisPersistentProperty>) association -> {

			String currentPath = !path.isEmpty() ? path + "." + association.getInverse().getName()
					: association.getInverse().getName();

			if (association.getInverse().isCollectionLike()) {

				Bucket bucket = source.getBucket().extract(currentPath + ".[");

				Collection<Object> target = CollectionFactory.createCollection(association.getInverse().getType(),
						association.getInverse().getComponentType(), bucket.size());

				for (Entry<String, byte[]> entry : bucket.entrySet()) {

					String referenceKey = fromBytes(entry.getValue(), String.class);

					if (!KeyspaceIdentifier.isValid(referenceKey)) {
						continue;
					}

					KeyspaceIdentifier identifier = KeyspaceIdentifier.of(referenceKey);
					Map<byte[], byte[]> rawHash = referenceResolver.resolveReference(identifier.getId(),
							identifier.getKeyspace());

					if (!CollectionUtils.isEmpty(rawHash)) {
						target.add(read(association.getInverse().getActualType(), new RedisData(rawHash)));
					}
				}

				accessor.setProperty(association.getInverse(), target);

			} else {

				byte[] binKey = source.getBucket().get(currentPath);
				if (binKey == null || binKey.length == 0) {
					return;
				}

				String referenceKey = fromBytes(binKey, String.class);
				if (KeyspaceIdentifier.isValid(referenceKey)) {

					KeyspaceIdentifier identifier = KeyspaceIdentifier.of(referenceKey);

					Map<byte[], byte[]> rawHash = referenceResolver.resolveReference(identifier.getId(),
							identifier.getKeyspace());

					if (!CollectionUtils.isEmpty(rawHash)) {
						accessor.setProperty(association.getInverse(),
								read(association.getInverse().getActualType(), new RedisData(rawHash)));
					}
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings({ "rawtypes" })
	public void write(Object source, RedisData sink) {

		if (source == null) {
			return;
		}

		if (source instanceof PartialUpdate) {
			writePartialUpdate((PartialUpdate) source, sink);
			return;
		}

		RedisPersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());

		if (!customConversions.hasCustomWriteTarget(source.getClass())) {
			typeMapper.writeType(ClassUtils.getUserClass(source), sink.getBucket().getPath());
		}

		if (entity == null) {

			typeMapper.writeType(ClassUtils.getUserClass(source), sink.getBucket().getPath());
			sink.getBucket().put("_raw", conversionService.convert(source, byte[].class));
			return;
		}

		sink.setKeyspace(entity.getKeySpace());

		writeInternal(entity.getKeySpace(), "", source, entity.getTypeInformation(), sink);

		Object identifier = entity.getIdentifierAccessor(source).getIdentifier();

		if (identifier != null) {
			sink.setId(getConversionService().convert(identifier, String.class));
		}

		Long ttl = entity.getTimeToLiveAccessor().getTimeToLive(source);
		if (ttl != null && ttl > 0) {
			sink.setTimeToLive(ttl);
		}

		for (IndexedData indexedData : indexResolver.resolveIndexesFor(entity.getTypeInformation(), source)) {
			sink.addIndexedData(indexedData);
		}
	}

	protected void writePartialUpdate(PartialUpdate<?> update, RedisData sink) {

		RedisPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(update.getTarget());

		write(update.getValue(), sink);

		for (String key : sink.getBucket().keySet()) {
			if (typeMapper.isTypeKey(key)) {
				sink.getBucket().remove(key);
				break;
			}
		}

		if (update.isRefreshTtl() && !update.getPropertyUpdates().isEmpty()) {

			Long ttl = entity.getTimeToLiveAccessor().getTimeToLive(update);
			if (ttl != null && ttl > 0) {
				sink.setTimeToLive(ttl);
			}
		}

		for (PropertyUpdate pUpdate : update.getPropertyUpdates()) {

			String path = pUpdate.getPropertyPath();

			if (UpdateCommand.SET.equals(pUpdate.getCmd())) {
				writePartialPropertyUpdate(update, pUpdate, sink, entity, path);
			}
		}
	}

	/**
	 * @param update
	 * @param pUpdate
	 * @param sink
	 * @param entity
	 * @param path
	 */
	private void writePartialPropertyUpdate(PartialUpdate<?> update, PropertyUpdate pUpdate, RedisData sink,
			RedisPersistentEntity<?> entity, String path) {

		RedisPersistentProperty targetProperty = getTargetPropertyOrNullForPath(path, update.getTarget());

		if (targetProperty == null) {

			targetProperty = getTargetPropertyOrNullForPath(path.replaceAll("\\.\\[.*\\]", ""), update.getTarget());

			TypeInformation<?> ti = targetProperty == null ? ClassTypeInformation.OBJECT
					: (targetProperty.isMap()
							? (targetProperty.getTypeInformation().getMapValueType() != null
									? targetProperty.getTypeInformation().getRequiredMapValueType() : ClassTypeInformation.OBJECT)
							: targetProperty.getTypeInformation().getActualType());

			writeInternal(entity.getKeySpace(), pUpdate.getPropertyPath(), pUpdate.getValue(), ti, sink);
			return;
		}

		if (targetProperty.isAssociation()) {

			if (targetProperty.isCollectionLike()) {

				RedisPersistentEntity<?> ref = mappingContext.getPersistentEntity(targetProperty.getRequiredAssociation()
						.getInverse().getTypeInformation().getRequiredComponentType().getActualType());

				int i = 0;
				for (Object o : (Collection<?>) pUpdate.getValue()) {

					Object refId = ref.getPropertyAccessor(o).getProperty(ref.getRequiredIdProperty());
					if (refId != null) {
						sink.getBucket().put(pUpdate.getPropertyPath() + ".[" + i + "]", toBytes(ref.getKeySpace() + ":" + refId));
						i++;
					}
				}
			} else {

				RedisPersistentEntity<?> ref = mappingContext
						.getRequiredPersistentEntity(targetProperty.getRequiredAssociation().getInverse().getTypeInformation());

				Object refId = ref.getPropertyAccessor(pUpdate.getValue()).getProperty(ref.getRequiredIdProperty());
				if (refId != null) {
					sink.getBucket().put(pUpdate.getPropertyPath(), toBytes(ref.getKeySpace() + ":" + refId));
				}
			}
		} else if (targetProperty.isCollectionLike()) {

			Collection<?> collection = pUpdate.getValue() instanceof Collection ? (Collection<?>) pUpdate.getValue()
					: Collections.<Object> singleton(pUpdate.getValue());
			writeCollection(entity.getKeySpace(), pUpdate.getPropertyPath(), collection,
					targetProperty.getTypeInformation().getActualType(), sink);
		} else if (targetProperty.isMap()) {

			Map<Object, Object> map = new HashMap<>();

			if (pUpdate.getValue() instanceof Map) {
				map.putAll((Map<?, ?>) pUpdate.getValue());
			} else if (pUpdate.getValue() instanceof Entry) {
				map.put(((Entry<?, ?>) pUpdate.getValue()).getKey(), ((Entry<?, ?>) pUpdate.getValue()).getValue());
			} else {
				throw new MappingException(
						String.format("Cannot set update value for map property '%s' to '%s'. Please use a Map or Map.Entry.",
								pUpdate.getPropertyPath(), pUpdate.getValue()));
			}

			writeMap(entity.getKeySpace(), pUpdate.getPropertyPath(), targetProperty.getMapValueType(), map, sink);
		} else {

			writeInternal(entity.getKeySpace(), pUpdate.getPropertyPath(), pUpdate.getValue(),
					targetProperty.getTypeInformation(), sink);

			Set<IndexedData> data = indexResolver.resolveIndexesFor(entity.getKeySpace(), pUpdate.getPropertyPath(),
					targetProperty.getTypeInformation(), pUpdate.getValue());

			if (data.isEmpty()) {

				data = indexResolver.resolveIndexesFor(entity.getKeySpace(), pUpdate.getPropertyPath(),
						targetProperty.getOwner().getTypeInformation(), pUpdate.getValue());

			}
			sink.addIndexedData(data);
		}
	}

	@Nullable
	RedisPersistentProperty getTargetPropertyOrNullForPath(String path, Class<?> type) {

		try {

			PersistentPropertyPath<RedisPersistentProperty> persistentPropertyPath = mappingContext
					.getPersistentPropertyPath(path, type);
			return persistentPropertyPath.getLeafProperty();
		} catch (Exception e) {
			// that's just fine
		}

		return null;
	}

	/**
	 * @param keyspace
	 * @param path
	 * @param value
	 * @param typeHint
	 * @param sink
	 */
	private void writeInternal(String keyspace, String path, @Nullable Object value, TypeInformation<?> typeHint,
			RedisData sink) {

		if (value == null) {
			return;
		}

		if (customConversions.hasCustomWriteTarget(value.getClass())) {

			if (!StringUtils.hasText(path) && customConversions.getCustomWriteTarget(value.getClass()).equals(byte[].class)) {
				sink.getBucket().put(StringUtils.hasText(path) ? path : "_raw", conversionService.convert(value, byte[].class));
			} else {

				if (!ClassUtils.isAssignable(typeHint.getType(), value.getClass())) {
					throw new MappingException(
							String.format(INVALID_TYPE_ASSIGNMENT, value.getClass(), path, typeHint.getType()));
				}
				writeToBucket(path, value, sink, typeHint.getType());
			}
			return;
		}

		if (value.getClass() != typeHint.getType()) {
			typeMapper.writeType(value.getClass(), sink.getBucket().getPropertyPath(path));
		}

		RedisPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(value.getClass());
		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);

		entity.doWithProperties((PropertyHandler<RedisPersistentProperty>) persistentProperty -> {

			String propertyStringPath = (!path.isEmpty() ? path + "." : "") + persistentProperty.getName();

			Object propertyValue = accessor.getProperty(persistentProperty);
			if (persistentProperty.isIdProperty()) {

				if (propertyValue != null) {
					sink.getBucket().put(propertyStringPath, toBytes(propertyValue));
				}
				return;
			}

			if (persistentProperty.isMap()) {

				if (propertyValue != null) {
					writeMap(keyspace, propertyStringPath, persistentProperty.getMapValueType(), (Map<?, ?>) propertyValue, sink);
				}
			} else if (persistentProperty.isCollectionLike()) {

				if (propertyValue == null) {
					writeCollection(keyspace, propertyStringPath, null,
							persistentProperty.getTypeInformation().getRequiredComponentType(), sink);
				} else {

					if (Iterable.class.isAssignableFrom(propertyValue.getClass())) {

						writeCollection(keyspace, propertyStringPath, (Iterable<?>) propertyValue,
								persistentProperty.getTypeInformation().getRequiredComponentType(), sink);
					} else if (propertyValue.getClass().isArray()) {

						writeCollection(keyspace, propertyStringPath, CollectionUtils.arrayToList(propertyValue),
								persistentProperty.getTypeInformation().getRequiredComponentType(), sink);
					} else {

						throw new RuntimeException("Don't know how to handle " + propertyValue.getClass() + " type collection");
					}
				}

			} else if (persistentProperty.isEntity()) {

				if (propertyValue != null) {
					writeInternal(keyspace, propertyStringPath, propertyValue,
							persistentProperty.getTypeInformation().getActualType(), sink);
				}
			} else {

				if (propertyValue != null) {
					writeToBucket(propertyStringPath, propertyValue, sink, persistentProperty.getType());
				}
			}
		});

		writeAssociation(path, entity, value, sink);
	}

	private void writeAssociation(String path, RedisPersistentEntity<?> entity, Object value, RedisData sink) {

		if (value == null) {
			return;
		}

		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);

		entity.doWithAssociations((AssociationHandler<RedisPersistentProperty>) association -> {

			Object refObject = accessor.getProperty(association.getInverse());
			if (refObject == null) {
				return;
			}

			if (association.getInverse().isCollectionLike()) {

				RedisPersistentEntity<?> ref = mappingContext.getRequiredPersistentEntity(
						association.getInverse().getTypeInformation().getRequiredComponentType().getActualType());

				String keyspace = ref.getKeySpace();
				String propertyStringPath = (!path.isEmpty() ? path + "." : "") + association.getInverse().getName();

				int i = 0;
				for (Object o : (Collection<?>) refObject) {

					Object refId = ref.getPropertyAccessor(o).getProperty(ref.getRequiredIdProperty());
					if (refId != null) {
						sink.getBucket().put(propertyStringPath + ".[" + i + "]", toBytes(keyspace + ":" + refId));
						i++;
					}
				}

			} else {

				RedisPersistentEntity<?> ref = mappingContext
						.getRequiredPersistentEntity(association.getInverse().getTypeInformation());
				String keyspace = ref.getKeySpace();

				Object refId = ref.getPropertyAccessor(refObject).getProperty(ref.getIdProperty());

				if (refId != null) {
					String propertyStringPath = (!path.isEmpty() ? path + "." : "") + association.getInverse().getName();
					sink.getBucket().put(propertyStringPath, toBytes(keyspace + ":" + refId));
				}
			}
		});
	}

	/**
	 * @param keyspace
	 * @param path
	 * @param values
	 * @param typeHint
	 * @param sink
	 */
	private void writeCollection(String keyspace, String path, @Nullable Iterable<?> values, TypeInformation<?> typeHint,
			RedisData sink) {

		if (values == null) {
			return;
		}

		int i = 0;
		for (Object value : values) {

			if (value == null) {
				break;
			}

			String currentPath = path + ".[" + i + "]";

			if (!ClassUtils.isAssignable(typeHint.getType(), value.getClass())) {
				throw new MappingException(
						String.format(INVALID_TYPE_ASSIGNMENT, value.getClass(), currentPath, typeHint.getType()));
			}

			if (customConversions.hasCustomWriteTarget(value.getClass())) {
				writeToBucket(currentPath, value, sink, typeHint.getType());
			} else {
				writeInternal(keyspace, currentPath, value, typeHint, sink);
			}
			i++;
		}
	}

	private void writeToBucket(String path, @Nullable Object value, RedisData sink, Class<?> propertyType) {

		if (value == null || (value instanceof Optional && !((Optional<?>) value).isPresent())) {
			return;
		}

		if (customConversions.hasCustomWriteTarget(value.getClass())) {

			Optional<Class<?>> targetType = customConversions.getCustomWriteTarget(value.getClass());

			if (!targetType.filter(it -> ClassUtils.isAssignable(Map.class, it)).isPresent()
					&& customConversions.isSimpleType(value.getClass()) && value.getClass() != propertyType) {
				typeMapper.writeType(value.getClass(), sink.getBucket().getPropertyPath(path));
			}

			if (targetType.filter(it -> ClassUtils.isAssignable(Map.class, it)).isPresent()) {

				Map<?, ?> map = (Map<?, ?>) conversionService.convert(value, targetType.get());
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					sink.getBucket().put(path + (StringUtils.hasText(path) ? "." : "") + entry.getKey(),
							toBytes(entry.getValue()));
				}
			} else if (targetType.filter(it -> ClassUtils.isAssignable(byte[].class, it)).isPresent()) {
				sink.getBucket().put(path, toBytes(value));
			} else {
				throw new IllegalArgumentException(
						String.format("Cannot convert value '%s' of type %s to bytes.", value, value.getClass()));
			}
		}
	}

	private Object readCollectionOrArray(String path, Class<?> collectionType, Class<?> valueType, Bucket bucket) {

		List<String> keys = new ArrayList<>(bucket.extractAllKeysFor(path));
		keys.sort(listKeyComparator);

		boolean isArray = collectionType.isArray();
		Class<?> collectionTypeToUse = isArray ? ArrayList.class : collectionType;
		Collection<Object> target = CollectionFactory.createCollection(collectionTypeToUse, valueType, keys.size());

		for (String key : keys) {

			if (typeMapper.isTypeKey(key)) {
				continue;
			}

			Bucket elementData = bucket.extract(key);

			TypeInformation<?> typeInformation = typeMapper.readType(elementData.getPropertyPath(key),
					ClassTypeInformation.from(valueType));

			Class<?> typeToUse = typeInformation.getType();
			if (conversionService.canConvert(byte[].class, typeToUse)) {
				target.add(fromBytes(elementData.get(key), typeToUse));
			} else {
				target.add(readInternal(key, typeToUse, new RedisData(elementData)));
			}
		}

		return isArray ? toArray(target, collectionType, valueType) : (target.isEmpty() ? null : target);
	}

	/**
	 * @param keyspace
	 * @param path
	 * @param mapValueType
	 * @param source
	 * @param sink
	 */
	private void writeMap(String keyspace, String path, Class<?> mapValueType, Map<?, ?> source, RedisData sink) {

		if (CollectionUtils.isEmpty(source)) {
			return;
		}

		for (Map.Entry<?, ?> entry : source.entrySet()) {

			if (entry.getValue() == null || entry.getKey() == null) {
				continue;
			}

			String currentPath = path + ".[" + mapMapKey(entry.getKey()) + "]";

			if (!ClassUtils.isAssignable(mapValueType, entry.getValue().getClass())) {
				throw new MappingException(
						String.format(INVALID_TYPE_ASSIGNMENT, entry.getValue().getClass(), currentPath, mapValueType));
			}

			if (customConversions.hasCustomWriteTarget(entry.getValue().getClass())) {
				writeToBucket(currentPath, entry.getValue(), sink, mapValueType);
			} else {
				writeInternal(keyspace, currentPath, entry.getValue(), ClassTypeInformation.from(mapValueType), sink);
			}
		}
	}

	private String mapMapKey(Object key) {

		if (conversionService.canConvert(key.getClass(), byte[].class)) {
			return new String(conversionService.convert(key, byte[].class));
		}

		return conversionService.convert(key, String.class);
	}

	/**
	 * @param path
	 * @param mapType
	 * @param keyType
	 * @param valueType
	 * @param source
	 * @return
	 */
	@Nullable
	private Map<?, ?> readMapOfSimpleTypes(String path, Class<?> mapType, Class<?> keyType, Class<?> valueType,
			RedisData source) {

		Bucket partial = source.getBucket().extract(path + ".[");

		Map<Object, Object> target = CollectionFactory.createMap(mapType, partial.size());

		for (Entry<String, byte[]> entry : partial.entrySet()) {

			if (typeMapper.isTypeKey(entry.getKey())) {
				continue;
			}

			Object key = extractMapKeyForPath(path, entry.getKey(), keyType);
			Class<?> typeToUse = getTypeHint(path + ".[" + key + "]", source.getBucket(), valueType);
			target.put(key, fromBytes(entry.getValue(), typeToUse));
		}

		return target.isEmpty() ? null : target;
	}

	/**
	 * @param path
	 * @param mapType
	 * @param keyType
	 * @param valueType
	 * @param source
	 * @return
	 */
	@Nullable
	private Map<?, ?> readMapOfComplexTypes(String path, Class<?> mapType, Class<?> keyType, Class<?> valueType,
			RedisData source) {

		Set<String> keys = source.getBucket().extractAllKeysFor(path);

		Map<Object, Object> target = CollectionFactory.createMap(mapType, keys.size());

		for (String key : keys) {

			Bucket partial = source.getBucket().extract(key);

			Object mapKey = extractMapKeyForPath(path, key, keyType);

			TypeInformation<?> typeInformation = typeMapper.readType(source.getBucket().getPropertyPath(key),
					ClassTypeInformation.from(valueType));

			Object o = readInternal(key, typeInformation.getType(), new RedisData(partial));
			target.put(mapKey, o);
		}

		return target.isEmpty() ? null : target;
	}

	private Object extractMapKeyForPath(String path, String key, Class<?> targetType) {

		String regex = "^(" + Pattern.quote(path) + "\\.\\[)(.*?)(\\])";
		Pattern pattern = Pattern.compile(regex);

		Matcher matcher = pattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException(
					String.format("Cannot extract map value for key '%s' in path '%s'.", key, path));
		}

		Object mapKey = matcher.group(2);

		if (ClassUtils.isAssignable(targetType, mapKey.getClass())) {
			return mapKey;
		}

		return conversionService.convert(toBytes(mapKey), targetType);
	}

	private Class<?> getTypeHint(String path, Bucket bucket, Class<?> fallback) {

		TypeInformation<?> typeInformation = typeMapper.readType(bucket.getPropertyPath(path),
				ClassTypeInformation.from(fallback));
		return typeInformation.getType();
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

		return conversionService.convert(source, byte[].class);
	}

	/**
	 * Convert given binary representation to desired target type using the underlying {@link ConversionService}.
	 *
	 * @param source
	 * @param type
	 * @return
	 * @throws ConverterNotFoundException
	 */
	public <T> T fromBytes(byte[] source, Class<T> type) {
		return conversionService.convert(source, type);
	}

	/**
	 * Converts a given {@link Collection} into an array considering primitive types.
	 *
	 * @param source {@link Collection} of values to be added to the array.
	 * @param arrayType {@link Class} of array.
	 * @param valueType to be used for conversion before setting the actual value.
	 * @return
	 */
	@Nullable
	private Object toArray(Collection<Object> source, Class<?> arrayType, Class<?> valueType) {

		if (source.isEmpty()) {
			return null;
		}

		if (!ClassUtils.isPrimitiveArray(arrayType)) {
			return source.toArray((Object[]) Array.newInstance(valueType, source.size()));
		}

		Object targetArray = Array.newInstance(valueType, source.size());
		Iterator<Object> iterator = source.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			Array.set(targetArray, i, conversionService.convert(iterator.next(), valueType));
			i++;
		}
		return i > 0 ? targetArray : null;
	}

	public void setIndexResolver(IndexResolver indexResolver) {
		this.indexResolver = indexResolver;
	}

	public void setReferenceResolver(ReferenceResolver referenceResolver) {
		this.referenceResolver = referenceResolver;
	}

	/**
	 * Set {@link CustomConversions} to be applied.
	 *
	 * @param customConversions
	 */
	public void setCustomConversions(@Nullable CustomConversions customConversions) {
		this.customConversions = customConversions != null ? customConversions : new RedisCustomConversions();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	@Override
	public RedisMappingContext getMappingContext() {
		return this.mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.RedisConverter#getIndexResolver()
	 */
	@Nullable
	@Override
	public IndexResolver getIndexResolver() {
		return this.indexResolver;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getConversionService()
	 */
	@Override
	public ConversionService getConversionService() {
		return this.conversionService;
	}

	@Override
	public void afterPropertiesSet() {
		this.initializeConverters();
	}

	private void initializeConverters() {
		customConversions.registerConvertersIn(conversionService);
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	@RequiredArgsConstructor
	private class ConverterAwareParameterValueProvider implements PropertyValueProvider<RedisPersistentProperty> {

		private final String path;
		private final RedisData source;
		private final ConversionService conversionService;

		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(RedisPersistentProperty property) {

			Object value = readProperty(path, source, property);

			if (value != null && !property.getActualType().isInstance(value)) {
				return (T) conversionService.convert(value, property.getActualType());
			}

			return (T) value;
		}
	}

	private enum NaturalOrderingKeyComparator implements Comparator<String> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(String s1, String s2) {

			int s1offset = 0;
			int s2offset = 0;

			while (s1offset < s1.length() && s2offset < s2.length()) {

				Part thisPart = extractPart(s1, s1offset);
				Part thatPart = extractPart(s2, s2offset);

				int result = thisPart.compareTo(thatPart);

				if (result != 0) {
					return result;
				}

				s1offset += thisPart.length();
				s2offset += thatPart.length();
			}

			return 0;
		}

		private Part extractPart(String source, int offset) {

			StringBuilder builder = new StringBuilder();

			char c = source.charAt(offset);
			builder.append(c);

			boolean isDigit = Character.isDigit(c);
			for (int i = offset + 1; i < source.length(); i++) {

				c = source.charAt(i);
				if ((isDigit && !Character.isDigit(c)) || (!isDigit && Character.isDigit(c))) {
					break;
				}
				builder.append(c);
			}

			return new Part(builder.toString(), isDigit);
		}

		private static class Part implements Comparable<Part> {

			private final String rawValue;
			private final @Nullable Long longValue;

			Part(String value, boolean isDigit) {

				this.rawValue = value;
				this.longValue = isDigit ? Long.valueOf(value) : null;
			}

			boolean isNumeric() {
				return longValue != null;
			}

			int length() {
				return rawValue.length();
			}

			/*
			 * (non-Javadoc)
			 * @see java.lang.Comparable#compareTo(java.lang.Object)
			 */
			@Override
			public int compareTo(Part that) {

				if (this.isNumeric() && that.isNumeric()) {
					return this.longValue.compareTo(that.longValue);
				}

				return this.rawValue.compareTo(that.rawValue);
			}
		}
	}

	/**
	 * Value object representing a Redis Hash/Object identifier composed from keyspace and object id in the form of
	 * {@literal keyspace:id}.
	 *
	 * @author Mark Paluch
	 * @since 1.8.10
	 */
	@AllArgsConstructor(access = AccessLevel.PRIVATE)
	@Getter
	public static class KeyspaceIdentifier {

		public static final String PHANTOM = "phantom";
		public static final String DELIMITTER = ":";
		public static final String PHANTOM_SUFFIX = DELIMITTER + PHANTOM;

		private String keyspace;
		private String id;
		private boolean phantomKey;

		/**
		 * Parse a {@code key} into {@link KeyspaceIdentifier}.
		 *
		 * @param key the key representation.
		 * @return {@link BinaryKeyspaceIdentifier} for binary key.
		 */
		public static KeyspaceIdentifier of(String key) {

			Assert.isTrue(isValid(key), String.format("Invalid key %s", key));

			boolean phantomKey = key.endsWith(PHANTOM_SUFFIX);
			int keyspaceEndIndex = key.indexOf(DELIMITTER);
			String keyspace = key.substring(0, keyspaceEndIndex);
			String id;

			if (phantomKey) {
				id = key.substring(keyspaceEndIndex + 1, key.length() - PHANTOM_SUFFIX.length());
			} else {
				id = key.substring(keyspaceEndIndex + 1);
			}

			return new KeyspaceIdentifier(keyspace, id, phantomKey);
		}

		/**
		 * Check whether the {@code key} is valid, in particular whether the key contains a keyspace and an id part in the
		 * form of {@literal keyspace:id}.
		 *
		 * @param key the key.
		 * @return {@literal true} if the key is valid.
		 */
		public static boolean isValid(String key) {

			if (key == null) {
				return false;
			}

			int keyspaceEndIndex = key.indexOf(DELIMITTER);

			return keyspaceEndIndex > 0 && key.length() > keyspaceEndIndex;
		}
	}

	/**
	 * Value object representing a binary Redis Hash/Object identifier composed from keyspace and object id in the form of
	 * {@literal keyspace:id}.
	 *
	 * @author Mark Paluch
	 * @since 1.8.10
	 */
	@AllArgsConstructor(access = AccessLevel.PRIVATE)
	@Getter
	public static class BinaryKeyspaceIdentifier {

		public static final byte[] PHANTOM = KeyspaceIdentifier.PHANTOM.getBytes();
		public static final byte DELIMITTER = ':';
		public static final byte[] PHANTOM_SUFFIX = ByteUtils.concat(new byte[] { DELIMITTER }, PHANTOM);

		private byte[] keyspace;
		private byte[] id;
		private boolean phantomKey;

		/**
		 * Parse a binary {@code key} into {@link BinaryKeyspaceIdentifier}.
		 *
		 * @param key the binary key representation.
		 * @return {@link BinaryKeyspaceIdentifier} for binary key.
		 */
		public static BinaryKeyspaceIdentifier of(byte[] key) {

			Assert.isTrue(isValid(key), String.format("Invalid key %s", new String(key)));

			boolean phantomKey = ByteUtils.startsWith(key, PHANTOM_SUFFIX, key.length - PHANTOM_SUFFIX.length);

			int keyspaceEndIndex = ByteUtils.indexOf(key, DELIMITTER);
			byte[] keyspace = extractKeyspace(key, keyspaceEndIndex);
			byte[] id = extractId(key, phantomKey, keyspaceEndIndex);

			return new BinaryKeyspaceIdentifier(keyspace, id, phantomKey);
		}

		/**
		 * Check whether the {@code key} is valid, in particular whether the key contains a keyspace and an id part in the
		 * form of {@literal keyspace:id}.
		 *
		 * @param key the key.
		 * @return {@literal true} if the key is valid.
		 */
		public static boolean isValid(byte[] key) {

			if (key == null) {
				return false;
			}

			int keyspaceEndIndex = ByteUtils.indexOf(key, DELIMITTER);

			return keyspaceEndIndex > 0 && key.length > keyspaceEndIndex;
		}

		private static byte[] extractId(byte[] key, boolean phantomKey, int keyspaceEndIndex) {

			int idSize;

			if (phantomKey) {
				idSize = (key.length - PHANTOM_SUFFIX.length) - (keyspaceEndIndex + 1);
			} else {

				idSize = key.length - (keyspaceEndIndex + 1);
			}

			byte[] id = new byte[idSize];
			System.arraycopy(key, keyspaceEndIndex + 1, id, 0, idSize);

			return id;
		}

		private static byte[] extractKeyspace(byte[] key, int keyspaceEndIndex) {

			byte[] keyspace = new byte[keyspaceEndIndex];
			System.arraycopy(key, 0, keyspace, 0, keyspaceEndIndex);

			return keyspace;
		}
	}
}
