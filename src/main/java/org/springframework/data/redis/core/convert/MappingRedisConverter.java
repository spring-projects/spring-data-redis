/*
 * Copyright 2015 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.IndexType;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.RedisIndexDefinition;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * {@link RedisConverter} implementation creating flat binary map structure out of a given domain type. Considers
 * {@link Indexed} annotation for enabling helper structures for finder operations.
 * 
 * <pre>
 * <code>
 * &#64;KeySpace("persons")
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
 */
public class MappingRedisConverter implements RedisConverter {

	private final KeyValueMappingContext mappingContext;
	private final GenericConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final TypeMapper<RedisData> typeMapper;
	private final ReferenceResolver referenceResolver;

	private final IndexConfiguration indexConfiguration;

	/**
	 * @param indexConfiguration can be {@literal null}.
	 * @param referenceResolver must not be {@literal null}.
	 */
	public MappingRedisConverter(IndexConfiguration indexConfiguration, ReferenceResolver referenceResolver) {

		Assert.notNull(referenceResolver, "ReferenceResolver must not be null!");

		mappingContext = new KeyValueMappingContext();
		entityInstantiators = new EntityInstantiators();

		this.conversionService = new DefaultConversionService();
		this.conversionService.addConverter(new BinaryConverters.StringToBytesConverter());
		this.conversionService.addConverter(new BinaryConverters.BytesToStringConverter());
		this.conversionService.addConverter(new BinaryConverters.NumberToBytesConverter());
		this.conversionService.addConverterFactory(new BinaryConverters.BytesToNumberConverterFactory());
		this.conversionService.addConverter(new BinaryConverters.EnumToBytesConverter());
		this.conversionService.addConverterFactory(new BinaryConverters.BytesToEnumConverterFactory());
		this.conversionService.addConverter(new BinaryConverters.BooleanToBytesConverter());
		this.conversionService.addConverter(new BinaryConverters.BytesToBooleanConverter());
		this.conversionService.addConverter(new BinaryConverters.DateToBytesConverter());
		this.conversionService.addConverter(new BinaryConverters.BytesToDateConverter());

		typeMapper = new DefaultTypeMapper<RedisData>(new RedisTypeAliasAccessor(this.conversionService));

		this.indexConfiguration = indexConfiguration != null ? indexConfiguration : new IndexConfiguration();
		this.referenceResolver = referenceResolver;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityReader#read(java.lang.Class, java.lang.Object)
	 */
	public <R> R read(Class<R> type, final RedisData source) {
		return readInternal("", type, source);
	}

	@SuppressWarnings("unchecked")
	private <R> R readInternal(final String path, Class<R> type, final RedisData source) {

		if (CollectionUtils.isEmpty(source.getData())) {
			return null;
		}

		TypeInformation<?> readType = typeMapper.readType(source);
		TypeInformation<?> typeToUse = readType != null ? readType : ClassTypeInformation.from(type);

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);

		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);

		Object instance = instantiator.createInstance((KeyValuePersistentEntity<?>) entity,
				new PersistentEntityParameterValueProvider<KeyValuePersistentProperty>(entity,
						new ConverterAwareParameterValueProvider(source, conversionService), null));

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

					if (conversionService.canConvert(byte[].class, persistentProperty.getMapValueType())) {
						accessor.setProperty(
								persistentProperty,
								readMapOfSimpleTypes(currentPath, persistentProperty.getType(), persistentProperty.getComponentType(),
										persistentProperty.getMapValueType(), source));
					} else {
						accessor.setProperty(
								persistentProperty,
								readMapOfComplexTypes(currentPath, persistentProperty.getType(), persistentProperty.getComponentType(),
										persistentProperty.getMapValueType(), source));
					}
				}

				else if (persistentProperty.isCollectionLike()) {

					if (conversionService.canConvert(byte[].class, persistentProperty.getComponentType())) {
						accessor.setProperty(
								persistentProperty,
								readCollectionOfSimpleTypes(currentPath, persistentProperty.getType(), persistentProperty
										.getTypeInformation().getComponentType().getActualType().getType(), source));
					} else {
						accessor.setProperty(
								persistentProperty,
								readCollectionOfComplexTypes(currentPath, persistentProperty.getType(), persistentProperty
										.getTypeInformation().getComponentType().getActualType().getType(), source));
					}

				} else if (persistentProperty.isEntity()) {

					Map<byte[], byte[]> raw = extractDataStartingWith(toBytes(currentPath + "."), source);
					RedisData source = new RedisData(raw);
					byte[] type = source.getDataForKey(toBytes(currentPath + "._class"));
					if (type != null && type.length > 0) {
						source.addDataEntry(toBytes("_class"), type);
					}

					Class<?> myType = persistentProperty.getTypeInformation().getActualType().getType();

					accessor.setProperty(persistentProperty, readInternal(currentPath, myType, source));

				} else {
					accessor.setProperty(persistentProperty,
							fromBytes(source.getDataForKey(toBytes(currentPath)), persistentProperty.getActualType()));
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

				String currentPath = !path.isEmpty() ? path + "." + association.getInverse().getName() : association
						.getInverse().getName();

				if (association.getInverse().isCollectionLike()) {

					Collection target = CollectionFactory.createCollection(association.getInverse().getType(), association
							.getInverse().getComponentType(), 10);

					Map<byte[], byte[]> values = extractDataStartingWith(toBytes(currentPath + ".["), source);
					for (Entry<byte[], byte[]> entry : values.entrySet()) {

						String key = fromBytes(entry.getValue(), String.class);
						String[] args = key.split(":");

						Object loadedObject = referenceResolver.resolveReference(args[1], args[0], association.getInverse()
								.getActualType());

						if (loadedObject != null) {
							target.add(loadedObject);
						}
					}

					accessor.setProperty(association.getInverse(), target);

				} else {

					byte[] binKey = source.getDataForKey(toBytes(currentPath));
					if (binKey == null || binKey.length == 0) {
						return;
					}

					String key = fromBytes(binKey, String.class);

					String[] args = key.split(":");
					Object loadedObject = referenceResolver.resolveReference(args[1], args[0], association.getInverse()
							.getActualType());

					if (loadedObject != null) {
						accessor.setProperty(association.getInverse(), loadedObject);
					}
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	public void write(Object source, final RedisData sink) {

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());

		typeMapper.writeType(ClassUtils.getUserClass(source), sink);
		sink.setKeyspace(toBytes(entity.getKeySpace()));

		writeInternal(entity.getKeySpace(), "", source, entity.getTypeInformation(), sink);
		sink.setId(toBytes(entity.getIdentifierAccessor(source).getIdentifier()));
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

		if (value.getClass() != typeHint.getType()) {
			sink.addDataEntry(toBytes((!path.isEmpty() ? path + "._class" : "_class")), toBytes(value.getClass().getName()));
		}

		final KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(value.getClass());
		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				String propertyStringPath = (!path.isEmpty() ? path + "." : "") + persistentProperty.getName();

				if (persistentProperty.isIdProperty()) {

					sink.addDataEntry(toBytes(persistentProperty.getName()), toBytes(accessor.getProperty(persistentProperty)));
					return;
				}

				if (persistentProperty.isMap()) {
					writeMap(keyspace, propertyStringPath, persistentProperty.getMapValueType(),
							(Map<?, ?>) accessor.getProperty(persistentProperty), sink);
				} else if (persistentProperty.isCollectionLike()) {
					writeCollection(keyspace, propertyStringPath, (Collection<?>) accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getComponentType(), sink);
				} else if (persistentProperty.isEntity()) {
					writeInternal(keyspace, propertyStringPath, accessor.getProperty(persistentProperty), persistentProperty
							.getTypeInformation().getActualType(), sink);
				} else {

					if (indexConfiguration.hasIndexFor(entity.getKeySpace(), persistentProperty.getName())) {

						// TODO: check all index types and add accordingly
						sink.addSimpleIndexKey(toBytes(persistentProperty.getName() + ":"
								+ accessor.getProperty(persistentProperty)));
					}

					else if (persistentProperty.isAnnotationPresent(Indexed.class)) {

						// TOOD: read index type from annotation
						indexConfiguration.addIndexDefinition(new RedisIndexDefinition(entity.getKeySpace(), persistentProperty
								.getName(), IndexType.SIMPLE));

						sink.addSimpleIndexKey(toBytes(persistentProperty.getName() + ":"
								+ accessor.getProperty(persistentProperty)));
					}
					sink.addDataEntry(toBytes(propertyStringPath), toBytes(accessor.getProperty(persistentProperty)));
				}
			}
		});

		writeAssiciation(keyspace, path, entity, value, sink);
	}

	private void writeAssiciation(final String keyspace, final String path, final KeyValuePersistentEntity<?> entity,
			final Object value, final RedisData sink) {

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

					KeyValuePersistentEntity<?> ref = mappingContext.getPersistentEntity(association.getInverse()
							.getTypeInformation().getComponentType().getActualType());

					String keyspace = ref.getKeySpace();
					String propertyStringPath = (!path.isEmpty() ? path + "." : "") + association.getInverse().getName();

					int i = 0;
					for (Object o : (Collection<?>) refObject) {

						Object refId = ref.getPropertyAccessor(o).getProperty(ref.getIdProperty());
						sink.addDataEntry(toBytes(propertyStringPath + ".[" + i + "]"), toBytes(keyspace + ":" + refId));
						i++;
					}

				} else {

					KeyValuePersistentEntity<?> ref = mappingContext.getPersistentEntity(association.getInverse()
							.getTypeInformation());
					String keyspace = ref.getKeySpace();

					Object refId = ref.getPropertyAccessor(refObject).getProperty(ref.getIdProperty());

					String propertyStringPath = (!path.isEmpty() ? path + "." : "") + association.getInverse().getName();
					sink.addDataEntry(toBytes(propertyStringPath), toBytes(keyspace + ":" + refId));
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
	private void writeCollection(String keyspace, String path, Collection<?> values, TypeInformation<?> typeHint,
			RedisData sink) {

		if (values == null) {
			return;
		}

		int i = 0;
		for (Object o : values) {

			String currentPath = path + ".[" + i + "]";

			if (conversionService.canConvert(o.getClass(), byte[].class)) {
				sink.addDataEntry(toBytes(currentPath), toBytes(o));
			} else {
				writeInternal(keyspace, currentPath, o, typeHint, sink);
			}
			i++;
		}
	}

	/**
	 * @param path
	 * @param collectionType
	 * @param valueType
	 * @param source
	 * @return
	 */
	private Collection<?> readCollectionOfSimpleTypes(String path, Class<?> collectionType, Class<?> valueType,
			RedisData source) {

		Collection<Object> target = CollectionFactory.createCollection(collectionType, valueType, 10);

		Map<byte[], byte[]> values = extractDataStartingWith(toBytes(path + ".["), source);

		for (byte[] value : values.values()) {
			target.add(fromBytes(value, valueType));
		}
		return target;
	}

	/**
	 * @param path
	 * @param collectionType
	 * @param valueType
	 * @param source
	 * @return
	 */
	private Collection<?> readCollectionOfComplexTypes(String path, Class<?> collectionType, Class<?> valueType,
			RedisData source) {

		Collection<Object> target = CollectionFactory.createCollection(collectionType, valueType, 10);

		Set<byte[]> values = getKeysStartingWith(toBytes(path + ".["), source);

		for (byte[] value : values) {

			RedisData newPartialSource = new RedisData(extractDataStartingWith(value, source));

			byte[] typeInfo = source.getDataForKey(ByteUtils.concat(value, toBytes("._class")));
			if (typeInfo != null && typeInfo.length > 0) {
				newPartialSource.addDataEntry(toBytes("_class"), typeInfo);
			}

			Object o = readInternal(fromBytes(value, String.class), valueType, newPartialSource);
			target.add(o);
		}
		return target;
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

			if (conversionService.canConvert(entry.getValue().getClass(), byte[].class)) {
				sink.addDataEntry(toBytes(currentPath), toBytes(entry.getValue()));
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

		Map<Object, Object> target = CollectionFactory.createMap(mapType, 10);

		byte[] prefix = toBytes(path + ".[");
		byte[] postfix = toBytes("]");

		Map<byte[], byte[]> values = extractDataStartingWith(toBytes(path + ".["), source);
		for (Entry<byte[], byte[]> entry : values.entrySet()) {

			byte[] binKey = ByteUtils.extract(entry.getKey(), prefix, postfix);

			Object key = fromBytes(binKey, keyType);
			target.put(key, fromBytes(entry.getValue(), valueType));
		}
		return target;
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

		Map<Object, Object> target = CollectionFactory.createMap(mapType, 10);

		byte[] prefix = toBytes(path + ".[");
		byte[] postfix = toBytes("]");
		Set<byte[]> keys = getKeysStartingWith(prefix, source);

		for (byte[] fullKey : keys) {

			byte[] binKey = ByteUtils.extract(fullKey, prefix, postfix);

			Object key = fromBytes(binKey, keyType);

			RedisData newPartialSource = new RedisData(extractDataStartingWith(fullKey, source));

			byte[] typeInfo = source.getDataForKey(ByteUtils.concat(fullKey, toBytes("._class")));
			if (typeInfo != null && typeInfo.length > 0) {
				newPartialSource.addDataEntry(toBytes("_class"), typeInfo);
			}

			Object o = readInternal(fromBytes(fullKey, String.class), valueType, newPartialSource);

			target.put(key, o);
		}
		return target;
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

	public byte[] convertToId(Object keyspace, Object id) {
		return ByteUtils.concatAll(toBytes(keyspace), RedisData.ID_SEPERATOR, toBytes(id));
	}

	public byte[] convertPathToSimpleIndexId(Object keyspace, Object path) {

		byte[] bytePath = toBytes(path);
		byte[] keyspaceBin = toBytes(keyspace);

		return ByteUtils.concatAll(keyspaceBin, RedisData.PATH_SEPERATOR, bytePath);
	}

	private Set<byte[]> getKeysStartingWith(byte[] prefix, RedisData source) {

		Set<ByteArrayWrapper> keys = new LinkedHashSet<ByteArrayWrapper>();

		for (byte[] key : extractDataStartingWith(prefix, source).keySet()) {

			for (int i = prefix.length; i < key.length; i++) {
				if (key[i] == ']') {
					keys.add(new ByteArrayWrapper(Arrays.copyOfRange(key, 0, i + 1)));
					break;
				}
			}
		}

		Set<byte[]> result = new LinkedHashSet<byte[]>();
		for (ByteArrayWrapper wrapper : keys) {
			result.add(wrapper.getArray());
		}
		return result;
	}

	private Map<byte[], byte[]> extractDataStartingWith(byte[] prefix, RedisData source) {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		for (Entry<byte[], byte[]> entry : source.getData().entrySet()) {

			if (ByteUtils.startsWith(entry.getKey(), prefix)) {
				map.put(entry.getKey(), entry.getValue());
			}
		}
		return map;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	public MappingContext<? extends KeyValuePersistentEntity<?>, KeyValuePersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getConversionService()
	 */
	public ConversionService getConversionService() {
		return this.conversionService;
	}

	/**
	 * @author Christoph Strobl
	 */
	private static class ConverterAwareParameterValueProvider implements
			PropertyValueProvider<KeyValuePersistentProperty> {

		private final RedisData source;
		private final ConversionService conversionService;

		public ConverterAwareParameterValueProvider(RedisData source, ConversionService conversionService) {
			this.source = source;
			this.conversionService = conversionService;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(KeyValuePersistentProperty property) {
			return (T) conversionService.convert(
					source.getDataForKey(conversionService.convert(property.getName(), byte[].class)), property.getActualType());
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	private static class RedisTypeAliasAccessor implements TypeAliasAccessor<RedisData> {

		private final byte[] typeKey;

		private final ConversionService conversionService;

		RedisTypeAliasAccessor(ConversionService conversionService) {
			this(conversionService, "_class");
		}

		RedisTypeAliasAccessor(ConversionService conversionService, String typeKey) {

			this.conversionService = conversionService;
			this.typeKey = conversionService.convert(typeKey, byte[].class);
		}

		@Override
		public Object readAliasFrom(RedisData source) {
			return conversionService.convert(source.getDataForKey(typeKey), String.class);
		}

		@Override
		public void writeTypeTo(RedisData sink, Object alias) {
			sink.addDataEntry(typeKey, conversionService.convert(alias, byte[].class));
		}
	}

}
