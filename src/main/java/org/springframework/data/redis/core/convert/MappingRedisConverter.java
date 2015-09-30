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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
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
	private final IndexResolver indexResolver;

	/**
	 * @param indexResolver can be {@literal null}.
	 * @param referenceResolver must not be {@literal null}.
	 */
	public MappingRedisConverter(IndexResolver indexResolver, ReferenceResolver referenceResolver) {

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

		this.referenceResolver = referenceResolver;
		this.indexResolver = indexResolver != null ? indexResolver : new IndexResolverImpl(new IndexConfiguration());
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

		if (source.getBucket() == null || source.getBucket().isEmpty()) {
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
										.getTypeInformation().getComponentType().getActualType().getType(), source.getBucket()));
					}

				} else if (persistentProperty.isEntity()) {

					Bucket bucket = source.getBucket().extract(currentPath + ".");

					RedisData source = new RedisData(bucket);

					byte[] type = bucket.get(currentPath + "._class");
					if (type != null && type.length > 0) {
						source.getBucket().put("_class", type);
					}

					Class<?> myType = persistentProperty.getTypeInformation().getActualType().getType();

					accessor.setProperty(persistentProperty, readInternal(currentPath, myType, source));

				} else {
					accessor.setProperty(persistentProperty,
							fromBytes(source.getBucket().get(currentPath), persistentProperty.getActualType()));
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

					Collection<Object> target = CollectionFactory.createCollection(association.getInverse().getType(),
							association.getInverse().getComponentType(), 10);

					Bucket bucket = source.getBucket().extract(currentPath + ".[");

					for (Entry<String, byte[]> entry : bucket.entrySet()) {

						String referenceKey = fromBytes(entry.getValue(), String.class);
						String[] args = referenceKey.split(":");

						Object loadedObject = referenceResolver.resolveReference(args[1], args[0], association.getInverse()
								.getActualType());

						if (loadedObject != null) {
							target.add(loadedObject);
						}
					}

					accessor.setProperty(association.getInverse(), target);

				} else {

					byte[] binKey = source.getBucket().get(currentPath);
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
		sink.setKeyspace(entity.getKeySpace());

		writeInternal(entity.getKeySpace(), "", source, entity.getTypeInformation(), sink);
		sink.setId((Serializable) entity.getIdentifierAccessor(source).getIdentifier());
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
			sink.getBucket().put((!path.isEmpty() ? path + "._class" : "_class"), toBytes(value.getClass().getName()));
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
					writeCollection(keyspace, propertyStringPath, (Collection<?>) accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getComponentType(), sink);
				} else if (persistentProperty.isEntity()) {
					writeInternal(keyspace, propertyStringPath, accessor.getProperty(persistentProperty), persistentProperty
							.getTypeInformation().getActualType(), sink);
				} else {

					Object propertyValue = accessor.getProperty(persistentProperty);

					IndexedData index = indexResolver.resolveIndex(keyspace, propertyStringPath, persistentProperty,
							propertyValue);
					if (index != null) {
						sink.addIndexedData(index);
					}

					sink.getBucket().put(propertyStringPath, toBytes(propertyValue));
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
						sink.getBucket().put(propertyStringPath + ".[" + i + "]", toBytes(keyspace + ":" + refId));
						i++;
					}

				} else {

					KeyValuePersistentEntity<?> ref = mappingContext.getPersistentEntity(association.getInverse()
							.getTypeInformation());
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
	private void writeCollection(String keyspace, String path, Collection<?> values, TypeInformation<?> typeHint,
			RedisData sink) {

		if (values == null) {
			return;
		}

		int i = 0;
		for (Object value : values) {

			String currentPath = path + ".[" + i + "]";

			if (conversionService.canConvert(value.getClass(), byte[].class)) {
				sink.getBucket().put(currentPath, toBytes(value));
			} else {
				writeInternal(keyspace, currentPath, value, typeHint, sink);
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

		Bucket partial = source.getBucket().extract(path + ".[");

		for (byte[] value : partial.values()) {
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
			Bucket source) {

		Collection<Object> target = CollectionFactory.createCollection(collectionType, valueType, 10);

		Set<String> keys = source.extractAllKeysFor(path);

		for (String key : keys) {

			Bucket elementData = source.extract(key);

			byte[] typeInfo = elementData.get(key + "._class");
			if (typeInfo != null && typeInfo.length > 0) {
				elementData.put("_class", typeInfo);
			}

			Object o = readInternal(key, valueType, new RedisData(elementData));
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
				sink.getBucket().put(currentPath, toBytes(entry.getValue()));
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

		Bucket partial = source.getBucket().extract(path + ".[");

		for (Entry<String, byte[]> entry : partial.entrySet()) {

			String regex = "^(" + Pattern.quote(path) + "\\.\\[)(.*?)(\\])";
			Pattern pattern = Pattern.compile(regex);

			Matcher matcher = pattern.matcher(entry.getKey());
			if (!matcher.find()) {
				throw new RuntimeException("baähhh");
			}
			String key = matcher.group(2);
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

		Set<String> keys = source.getBucket().extractAllKeysFor(path);

		for (String key : keys) {

			String regex = "^(" + Pattern.quote(path) + "\\.\\[)(.*?)(\\])";
			Pattern pattern = Pattern.compile(regex);

			Matcher matcher = pattern.matcher(key);
			if (!matcher.find()) {
				throw new RuntimeException("baähhh");
			}
			String mapKey = matcher.group(2);

			Bucket partial = source.getBucket().extract(key);

			byte[] typeInfo = partial.get(key + "._class");
			if (typeInfo != null && typeInfo.length > 0) {
				partial.put("_class", typeInfo);
			}

			Object o = readInternal(key, valueType, new RedisData(partial));
			target.put(mapKey, o);
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
			return (T) conversionService.convert(source.getBucket().get(property.getName()), property.getActualType());
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	private static class RedisTypeAliasAccessor implements TypeAliasAccessor<RedisData> {

		private final String typeKey;

		private final ConversionService conversionService;

		RedisTypeAliasAccessor(ConversionService conversionService) {
			this(conversionService, "_class");
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

}
