/*
 * Copyright 2015-2017 the original author or authors.
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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.PartialUpdate.PropertyUpdate;
import org.springframework.data.redis.core.PartialUpdate.UpdateCommand;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
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

	private static final String TYPE_HINT_ALIAS = "_class";
	private static final String INVALID_TYPE_ASSIGNMENT = "Value of type %s cannot be assigned to property %s of type %s.";

	private final RedisMappingContext mappingContext;
	private final GenericConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final TypeMapper<RedisData> typeMapper;
	private final Comparator<String> listKeyComparator = new NullSafeComparator<String>(
			NaturalOrderingKeyComparator.INSTANCE, true);

	private ReferenceResolver referenceResolver;
	private IndexResolver indexResolver;
	private CustomConversions customConversions;

	/**
	 * Creates new {@link MappingRedisConverter}.
	 *
	 * @param context can be {@literal null}.
	 */
	MappingRedisConverter(RedisMappingContext context) {
		this(context, null, null);
	}

	/**
	 * Creates new {@link MappingRedisConverter} and defaults {@link RedisMappingContext} when {@literal null}.
	 *
	 * @param mappingContext can be {@literal null}.
	 * @param indexResolver can be {@literal null}.
	 * @param referenceResolver must not be {@literal null}.
	 */
	public MappingRedisConverter(RedisMappingContext mappingContext, IndexResolver indexResolver,
			ReferenceResolver referenceResolver) {

		this.mappingContext = mappingContext != null ? mappingContext : new RedisMappingContext();

		entityInstantiators = new EntityInstantiators();

		this.conversionService = new DefaultConversionService();
		this.customConversions = new CustomConversions();

		typeMapper = new DefaultTypeMapper<RedisData>(new RedisTypeAliasAccessor(this.conversionService));

		this.referenceResolver = referenceResolver;

		this.indexResolver = indexResolver != null ? indexResolver : new PathIndexResolver(this.mappingContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityReader#read(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <R> R read(Class<R> type, final RedisData source) {
		return readInternal("", type, source);
	}

	@SuppressWarnings("unchecked")
	private <R> R readInternal(final String path, Class<R> type, final RedisData source) {

		if (source.getBucket() == null || source.getBucket().isEmpty()) {
			return null;
		}

		TypeInformation<?> readType = typeMapper.readType(source);
		TypeInformation<?> typeToUse = readType != null ? readType : ClassTypeInformation.from(type);
		final RedisPersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);

		if (customConversions.hasCustomReadTarget(Map.class, typeToUse.getType())) {

			Map<String, byte[]> partial = new HashMap<String, byte[]>();

			if (!path.isEmpty()) {

				for (Entry<String, byte[]> entry : source.getBucket().extract(path + ".").entrySet()) {
					partial.put(entry.getKey().substring(path.length() + 1), entry.getValue());
				}

			} else {
				partial.putAll(source.getBucket().asMap());
			}
			R instance = (R) conversionService.convert(partial, typeToUse.getType());

			if (entity.hasIdProperty()) {
				entity.getPropertyAccessor(instance).setProperty(entity.getIdProperty(), source.getId());
			}
			return instance;
		}

		if (conversionService.canConvert(byte[].class, typeToUse.getType())) {
			return (R) conversionService.convert(source.getBucket().get(StringUtils.hasText(path) ? path : "_raw"),
					typeToUse.getType());
		}

		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);

		Object instance = instantiator.createInstance((RedisPersistentEntity<?>) entity,
				new PersistentEntityParameterValueProvider<KeyValuePersistentProperty>(entity,
						new ConverterAwareParameterValueProvider(path, source, conversionService), this.conversionService));

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(instance);

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				String currentPath = !path.isEmpty() ? path + "." + persistentProperty.getName() : persistentProperty.getName();

				PreferredConstructor<?, KeyValuePersistentProperty> constructor = entity.getPersistenceConstructor();

				if (constructor.isConstructorParameter(persistentProperty)) {
					return;
				}

				if (persistentProperty.isMap()) {

					Map<?, ?> targetValue = null;

					if (conversionService.canConvert(byte[].class, persistentProperty.getMapValueType())) {
						targetValue = readMapOfSimpleTypes(currentPath, persistentProperty.getType(),
								persistentProperty.getComponentType(), persistentProperty.getMapValueType(), source);
					} else {
						targetValue = readMapOfComplexTypes(currentPath, persistentProperty.getType(),
								persistentProperty.getComponentType(), persistentProperty.getMapValueType(), source);
					}

					if (targetValue != null) {
						accessor.setProperty(persistentProperty, targetValue);
					}
				}

				else if (persistentProperty.isCollectionLike()) {

					Object targetValue = readCollectionOrArray(currentPath, persistentProperty.getType(),
							persistentProperty.getTypeInformation().getComponentType().getActualType().getType(), source.getBucket());
					if (targetValue != null) {
						accessor.setProperty(persistentProperty, targetValue);
					}

				} else if (persistentProperty.isEntity() && !conversionService.canConvert(byte[].class,
						persistentProperty.getTypeInformation().getActualType().getType())) {

					Class<?> targetType = persistentProperty.getTypeInformation().getActualType().getType();

					Bucket bucket = source.getBucket().extract(currentPath + ".");

					RedisData newBucket = new RedisData(bucket);

					byte[] type = bucket.get(currentPath + "." + TYPE_HINT_ALIAS);
					if (type != null && type.length > 0) {
						newBucket.getBucket().put(TYPE_HINT_ALIAS, type);
					}

					accessor.setProperty(persistentProperty, readInternal(currentPath, targetType, newBucket));
				} else {

					if (persistentProperty.isIdProperty() && StringUtils.isEmpty(path.isEmpty())) {

						if (source.getBucket().get(currentPath) == null) {
							accessor.setProperty(persistentProperty,
									fromBytes(source.getBucket().get(currentPath), persistentProperty.getActualType()));
						} else {
							accessor.setProperty(persistentProperty, source.getId());
						}
					}

					Class<?> typeToUse = getTypeHint(currentPath, source.getBucket(), persistentProperty.getActualType());
					accessor.setProperty(persistentProperty, fromBytes(source.getBucket().get(currentPath), typeToUse));
				}
			}

		});

		readAssociation(path, source, entity, accessor);

		return (R) instance;
	}

	private void readAssociation(final String path, final RedisData source, final KeyValuePersistentEntity<?> entity,
			final PersistentPropertyAccessor accessor) {

		entity.doWithAssociations(new AssociationHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithAssociation(Association<KeyValuePersistentProperty> association) {

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
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	@SuppressWarnings({ "rawtypes" })
	public void write(Object source, final RedisData sink) {

		if (source == null) {
			return;
		}

		if (source instanceof PartialUpdate) {
			writePartialUpdate((PartialUpdate) source, sink);
			return;
		}

		final RedisPersistentEntity entity = mappingContext.getPersistentEntity(source.getClass());

		if (!customConversions.hasCustomWriteTarget(source.getClass())) {
			typeMapper.writeType(ClassUtils.getUserClass(source), sink);
		}

		if (entity == null) {

			typeMapper.writeType(ClassUtils.getUserClass(source), sink);
			sink.getBucket().put("_raw", conversionService.convert(source, byte[].class));
			return;
		}

		sink.setKeyspace(entity.getKeySpace());

		writeInternal(entity.getKeySpace(), "", source, entity.getTypeInformation(), sink);
		sink.setId(getConversionService().convert(entity.getIdentifierAccessor(source).getIdentifier(), String.class));

		Long ttl = entity.getTimeToLiveAccessor().getTimeToLive(source);
		if (ttl != null && ttl > 0) {
			sink.setTimeToLive(ttl);
		}

		for (IndexedData indexedData : indexResolver.resolveIndexesFor(entity.getTypeInformation(), source)) {
			sink.addIndexedData(indexedData);
		}
	}

	protected void writePartialUpdate(PartialUpdate<?> update, RedisData sink) {

		RedisPersistentEntity<?> entity = mappingContext.getPersistentEntity(update.getTarget());

		write(update.getValue(), sink);
		if (sink.getBucket().keySet().contains(TYPE_HINT_ALIAS)) {
			sink.getBucket().put(TYPE_HINT_ALIAS, null); // overwrite stuff in here
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

		KeyValuePersistentProperty targetProperty = getTargetPropertyOrNullForPath(path, update.getTarget());

		if (targetProperty == null) {

			targetProperty = getTargetPropertyOrNullForPath(path.replaceAll("\\.\\[.*\\]", ""), update.getTarget());

			TypeInformation<?> ti = targetProperty == null ? ClassTypeInformation.OBJECT
					: (targetProperty.isMap()
							? (targetProperty.getTypeInformation().getMapValueType() != null
									? targetProperty.getTypeInformation().getMapValueType() : ClassTypeInformation.OBJECT)
							: targetProperty.getTypeInformation().getActualType());

			writeInternal(entity.getKeySpace(), pUpdate.getPropertyPath(), pUpdate.getValue(), ti, sink);
			return;
		}

		if (targetProperty.isAssociation()) {

			if (targetProperty.isCollectionLike()) {

				KeyValuePersistentEntity<?> ref = mappingContext.getPersistentEntity(
						targetProperty.getAssociation().getInverse().getTypeInformation().getComponentType().getActualType());

				int i = 0;
				for (Object o : (Collection<?>) pUpdate.getValue()) {

					Object refId = ref.getPropertyAccessor(o).getProperty(ref.getIdProperty());
					sink.getBucket().put(pUpdate.getPropertyPath() + ".[" + i + "]", toBytes(ref.getKeySpace() + ":" + refId));
					i++;
				}
			} else {

				KeyValuePersistentEntity<?> ref = mappingContext
						.getPersistentEntity(targetProperty.getAssociation().getInverse().getTypeInformation());

				Object refId = ref.getPropertyAccessor(pUpdate.getValue()).getProperty(ref.getIdProperty());
				sink.getBucket().put(pUpdate.getPropertyPath(), toBytes(ref.getKeySpace() + ":" + refId));
			}
		} else if (targetProperty.isCollectionLike()) {

			Collection<?> collection = pUpdate.getValue() instanceof Collection ? (Collection<?>) pUpdate.getValue()
					: Collections.<Object> singleton(pUpdate.getValue());
			writeCollection(entity.getKeySpace(), pUpdate.getPropertyPath(), collection,
					targetProperty.getTypeInformation().getActualType(), sink);
		} else if (targetProperty.isMap()) {

			Map<Object, Object> map = new HashMap<Object, Object>();

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

	KeyValuePersistentProperty getTargetPropertyOrNullForPath(String path, Class<?> type) {

		try {

			PersistentPropertyPath<KeyValuePersistentProperty> persistentPropertyPath = mappingContext
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
	private void writeInternal(final String keyspace, final String path, final Object value, TypeInformation<?> typeHint,
			final RedisData sink) {

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
			sink.getBucket().put((!path.isEmpty() ? path + "." + TYPE_HINT_ALIAS : TYPE_HINT_ALIAS),
					toBytes(value.getClass().getName()));
		}

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(value.getClass());
		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				String propertyStringPath = (!path.isEmpty() ? path + "." : "") + persistentProperty.getName();

				if (persistentProperty.isIdProperty()) {

					sink.getBucket().put(propertyStringPath, toBytes(accessor.getProperty(persistentProperty)));
					return;
				}

				if (persistentProperty.isMap()) {
					writeMap(keyspace, propertyStringPath, persistentProperty.getMapValueType(),
							(Map<?, ?>) accessor.getProperty(persistentProperty), sink);
				} else if (persistentProperty.isCollectionLike()) {

					final Object property = accessor.getProperty(persistentProperty);

					if (property == null || Iterable.class.isAssignableFrom(property.getClass())) {

						writeCollection(keyspace, propertyStringPath, (Iterable<?>) property,
								persistentProperty.getTypeInformation().getComponentType(), sink);
					} else if (property.getClass().isArray()) {

						writeCollection(keyspace, propertyStringPath, CollectionUtils.arrayToList(property),
								persistentProperty.getTypeInformation().getComponentType(), sink);
					} else {

						throw new RuntimeException("Don't know how to handle " + property.getClass() + " type collection");
					}

				} else if (persistentProperty.isEntity()) {
					writeInternal(keyspace, propertyStringPath, accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getActualType(), sink);
				} else {

					Object propertyValue = accessor.getProperty(persistentProperty);
					writeToBucket(propertyStringPath, propertyValue, sink, persistentProperty.getType());
				}
			}
		});

		writeAssociation(path, entity, value, sink);
	}

	private void writeAssociation(final String path, final KeyValuePersistentEntity<?> entity, final Object value,
			final RedisData sink) {

		if (value == null) {
			return;
		}

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);

		entity.doWithAssociations(new AssociationHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithAssociation(Association<KeyValuePersistentProperty> association) {

				Object refObject = accessor.getProperty(association.getInverse());
				if (refObject == null) {
					return;
				}

				if (association.getInverse().isCollectionLike()) {

					KeyValuePersistentEntity<?> ref = mappingContext
							.getPersistentEntity(association.getInverse().getTypeInformation().getComponentType().getActualType());

					String keyspace = ref.getKeySpace();
					String propertyStringPath = (!path.isEmpty() ? path + "." : "") + association.getInverse().getName();

					int i = 0;
					for (Object o : (Collection<?>) refObject) {

						Object refId = ref.getPropertyAccessor(o).getProperty(ref.getIdProperty());
						sink.getBucket().put(propertyStringPath + ".[" + i + "]", toBytes(keyspace + ":" + refId));
						i++;
					}

				} else {

					KeyValuePersistentEntity<?> ref = mappingContext
							.getPersistentEntity(association.getInverse().getTypeInformation());
					String keyspace = ref.getKeySpace();

					Object refId = ref.getPropertyAccessor(refObject).getProperty(ref.getIdProperty());

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
	private void writeCollection(String keyspace, String path, Iterable<?> values, TypeInformation<?> typeHint,
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

	private void writeToBucket(String path, Object value, RedisData sink, Class<?> propertyType) {

		if (value == null) {
			return;
		}

		if (customConversions.hasCustomWriteTarget(value.getClass())) {

			Class<?> targetType = customConversions.getCustomWriteTarget(value.getClass());

			if (!ClassUtils.isAssignable(Map.class, targetType) && customConversions.isSimpleType(value.getClass())
					&& value.getClass() != propertyType) {
				sink.getBucket().put((!path.isEmpty() ? path + "." + TYPE_HINT_ALIAS : TYPE_HINT_ALIAS),
						toBytes(value.getClass().getName()));
			}

			if (ClassUtils.isAssignable(Map.class, targetType)) {

				Map<?, ?> map = (Map<?, ?>) conversionService.convert(value, targetType);
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					sink.getBucket().put(path + (StringUtils.hasText(path) ? "." : "") + entry.getKey(),
							toBytes(entry.getValue()));
				}
			} else if (ClassUtils.isAssignable(byte[].class, targetType)) {
				sink.getBucket().put(path, toBytes(value));
			} else {
				throw new IllegalArgumentException(
						String.format("Cannot convert value '%s' of type %s to bytes.", value, value.getClass()));
			}
		}

	}

	private Object readCollectionOrArray(String path, Class<?> collectionType, Class<?> valueType, Bucket bucket) {

		List<String> keys = new ArrayList<String>(bucket.extractAllKeysFor(path));
		Collections.sort(keys, listKeyComparator);

		boolean isArray = collectionType.isArray();
		Class<?> collectionTypeToUse = isArray ? ArrayList.class : collectionType;
		Collection<Object> target = CollectionFactory.createCollection(collectionTypeToUse, valueType, keys.size());

		for (String key : keys) {

			if (key.endsWith(TYPE_HINT_ALIAS)) {
				continue;
			}

			Bucket elementData = bucket.extract(key);

			byte[] typeInfo = elementData.get(key + "." + TYPE_HINT_ALIAS);
			if (typeInfo != null && typeInfo.length > 0) {
				elementData.put(TYPE_HINT_ALIAS, typeInfo);
			}

			Class<?> typeToUse = getTypeHint(key, elementData, valueType);
			if (conversionService.canConvert(byte[].class, typeToUse)) {
				target.add(fromBytes(elementData.get(key), typeToUse));
			} else {
				target.add(readInternal(key, valueType, new RedisData(elementData)));
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

			String currentPath = path + ".[" + entry.getKey() + "]";

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

	/**
	 * @param path
	 * @param mapType
	 * @param keyType
	 * @param valueType
	 * @param source
	 * @return
	 */
	private Map<?, ?> readMapOfSimpleTypes(String path, Class<?> mapType, Class<?> keyType, Class<?> valueType,
			RedisData source) {

		Bucket partial = source.getBucket().extract(path + ".[");

		Map<Object, Object> target = CollectionFactory.createMap(mapType, partial.size());

		for (Entry<String, byte[]> entry : partial.entrySet()) {

			if (entry.getKey().endsWith(TYPE_HINT_ALIAS)) {
				continue;
			}

			String regex = "^(" + Pattern.quote(path) + "\\.\\[)(.*?)(\\])";
			Pattern pattern = Pattern.compile(regex);

			Matcher matcher = pattern.matcher(entry.getKey());
			if (!matcher.find()) {
				throw new IllegalArgumentException(
						String.format("Cannot extract map value for key '%s' in path '%s'.", entry.getKey(), path));
			}
			String key = matcher.group(2);

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
	private Map<?, ?> readMapOfComplexTypes(String path, Class<?> mapType, Class<?> keyType, Class<?> valueType,
			RedisData source) {

		Set<String> keys = source.getBucket().extractAllKeysFor(path);

		Map<Object, Object> target = CollectionFactory.createMap(mapType, keys.size());

		for (String key : keys) {

			String regex = "^(" + Pattern.quote(path) + "\\.\\[)(.*?)(\\])";
			Pattern pattern = Pattern.compile(regex);

			Matcher matcher = pattern.matcher(key);
			if (!matcher.find()) {
				throw new IllegalArgumentException(
						String.format("Cannot extract map value for key '%s' in path '%s'.", key, path));
			}
			String mapKey = matcher.group(2);

			Bucket partial = source.getBucket().extract(key);

			byte[] typeInfo = partial.get(key + "." + TYPE_HINT_ALIAS);
			if (typeInfo != null && typeInfo.length > 0) {
				partial.put(TYPE_HINT_ALIAS, typeInfo);
			}

			Object o = readInternal(key, valueType, new RedisData(partial));
			target.put(mapKey, o);
		}

		return target.isEmpty() ? null : target;
	}

	private Class<?> getTypeHint(String path, Bucket bucket, Class<?> fallback) {

		byte[] typeInfo = bucket.get(path + "." + TYPE_HINT_ALIAS);

		if (typeInfo == null || typeInfo.length < 1) {
			return fallback;
		}

		String typeName = fromBytes(typeInfo, String.class);
		try {
			return ClassUtils.forName(typeName, this.getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new MappingException(String.format("Cannot find class for type %s. ", typeName), e);
		} catch (LinkageError e) {
			throw new MappingException(String.format("Cannot find class for type %s. ", typeName), e);
		}
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

	/**
	 * Set {@link CustomConversions} to be applied.
	 *
	 * @param customConversions
	 */
	public void setCustomConversions(CustomConversions customConversions) {
		this.customConversions = customConversions != null ? customConversions : new CustomConversions();
	}

	public void setReferenceResolver(ReferenceResolver referenceResolver) {
		this.referenceResolver = referenceResolver;
	}

	public void setIndexResolver(IndexResolver indexResolver) {
		this.indexResolver = indexResolver;
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
	 */
	private static class ConverterAwareParameterValueProvider
			implements PropertyValueProvider<KeyValuePersistentProperty> {

		private final String path;
		private final RedisData source;
		private final ConversionService conversionService;

		public ConverterAwareParameterValueProvider(String path, RedisData source, ConversionService conversionService) {

			this.path = path;
			this.source = source;
			this.conversionService = conversionService;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(KeyValuePersistentProperty property) {

			String name = StringUtils.hasText(path) ? path + "." + property.getName() : property.getName();
			return (T) conversionService.convert(source.getBucket().get(name), property.getActualType());
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	private static class RedisTypeAliasAccessor implements TypeAliasAccessor<RedisData> {

		private final String typeKey;

		private final ConversionService conversionService;

		RedisTypeAliasAccessor(ConversionService conversionService) {
			this(conversionService, TYPE_HINT_ALIAS);
		}

		RedisTypeAliasAccessor(ConversionService conversionService, String typeKey) {

			this.conversionService = conversionService;
			this.typeKey = typeKey;
		}

		@Override
		public Object readAliasFrom(RedisData source) {
			return conversionService.convert(source.getBucket().get(typeKey), String.class);
		}

		@Override
		public void writeTypeTo(RedisData sink, Object alias) {
			sink.getBucket().put(typeKey, conversionService.convert(alias, byte[].class));
		}
	}

	enum ClassNameKeySpaceResolver implements KeySpaceResolver {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.keyvalue.core.KeySpaceResolver#resolveKeySpace(java.lang.Class)
		 */
		@Override
		public String resolveKeySpace(Class<?> type) {

			Assert.notNull(type, "Type must not be null!");
			return ClassUtils.getUserClass(type).getName();
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
			private final Long longValue;

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

			boolean phantomKey = startsWith(key, PHANTOM_SUFFIX, key.length - PHANTOM_SUFFIX.length);

			int keyspaceEndIndex = find(key, DELIMITTER);
			byte[] keyspace = getKeyspace(key, keyspaceEndIndex);
			byte[] id = getId(key, phantomKey, keyspaceEndIndex);

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

			int keyspaceEndIndex = find(key, DELIMITTER);

			return keyspaceEndIndex > 0 && key.length > keyspaceEndIndex;
		}

		private static byte[] getId(byte[] key, boolean phantomKey, int keyspaceEndIndex) {

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

		private static byte[] getKeyspace(byte[] key, int keyspaceEndIndex) {

			byte[] keyspace = new byte[keyspaceEndIndex];
			System.arraycopy(key, 0, keyspace, 0, keyspaceEndIndex);

			return keyspace;
		}

		private static boolean startsWith(byte[] haystack, byte[] prefix, int offset) {

			int to = offset;
			int prefixOffset = 0;
			int prefixLength = prefix.length;

			if ((offset < 0) || (offset > haystack.length - prefixLength)) {
				return false;
			}

			while (--prefixLength >= 0) {
				if (haystack[to++] != prefix[prefixOffset++]) {
					return false;
				}
			}

			return true;
		}

		private static int find(byte[] haystack, byte needle) {

			for (int i = 0; i < haystack.length; i++) {
				if (haystack[i] == needle) {
					return i;
				}
			}

			return -1;
		}
	}
}
