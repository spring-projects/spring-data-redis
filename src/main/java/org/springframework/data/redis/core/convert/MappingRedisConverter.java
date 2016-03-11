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
package org.springframework.data.redis.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
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
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
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
 * @since 1.7
 */
public class MappingRedisConverter implements RedisConverter, InitializingBean {

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
						accessor.setProperty(persistentProperty, readMapOfSimpleTypes(currentPath, persistentProperty.getType(),
								persistentProperty.getComponentType(), persistentProperty.getMapValueType(), source));
					} else {
						accessor.setProperty(persistentProperty, readMapOfComplexTypes(currentPath, persistentProperty.getType(),
								persistentProperty.getComponentType(), persistentProperty.getMapValueType(), source));
					}
				}

				else if (persistentProperty.isCollectionLike()) {

					if (conversionService.canConvert(byte[].class, persistentProperty.getComponentType())) {
						accessor.setProperty(persistentProperty,
								readCollectionOfSimpleTypes(currentPath, persistentProperty.getType(),
										persistentProperty.getTypeInformation().getComponentType().getActualType().getType(), source));
					} else {
						accessor.setProperty(persistentProperty,
								readCollectionOfComplexTypes(currentPath, persistentProperty.getType(),
										persistentProperty.getTypeInformation().getComponentType().getActualType().getType(),
										source.getBucket()));
					}

				} else if (persistentProperty.isEntity() && !conversionService.canConvert(byte[].class,
						persistentProperty.getTypeInformation().getActualType().getType())) {

					Class<?> targetType = persistentProperty.getTypeInformation().getActualType().getType();

					Bucket bucket = source.getBucket().extract(currentPath + ".");

					RedisData source = new RedisData(bucket);

					byte[] type = bucket.get(currentPath + "._class");
					if (type != null && type.length > 0) {
						source.getBucket().put("_class", type);
					}

					accessor.setProperty(persistentProperty, readInternal(currentPath, targetType, source));
				} else {

					if (persistentProperty.isIdProperty() && StringUtils.isEmpty(path.isEmpty())) {

						if (source.getBucket().get(currentPath) == null) {
							accessor.setProperty(persistentProperty,
									fromBytes(source.getBucket().get(currentPath), persistentProperty.getActualType()));
						} else {
							accessor.setProperty(persistentProperty, source.getId());
						}
					}

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

				String currentPath = !path.isEmpty() ? path + "." + association.getInverse().getName()
						: association.getInverse().getName();

				if (association.getInverse().isCollectionLike()) {

					Bucket bucket = source.getBucket().extract(currentPath + ".[");

					Collection<Object> target = CollectionFactory.createCollection(association.getInverse().getType(),
							association.getInverse().getComponentType(), bucket.size());

					for (Entry<String, byte[]> entry : bucket.entrySet()) {

						String referenceKey = fromBytes(entry.getValue(), String.class);
						String[] args = referenceKey.split(":");

						Map<byte[], byte[]> rawHash = referenceResolver.resolveReference(args[1], args[0]);

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

					String key = fromBytes(binKey, String.class);

					String[] args = key.split(":");

					Map<byte[], byte[]> rawHash = referenceResolver.resolveReference(args[1], args[0]);

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
	public void write(Object source, final RedisData sink) {

		final RedisPersistentEntity entity = mappingContext.getPersistentEntity(source.getClass());

		if (!customConversions.hasCustomWriteTarget(source.getClass())) {
			typeMapper.writeType(ClassUtils.getUserClass(source), sink);
		}
		sink.setKeyspace(entity.getKeySpace());

		writeInternal(entity.getKeySpace(), "", source, entity.getTypeInformation(), sink);
		sink.setId(getConversionService().convert(entity.getIdentifierAccessor(source).getIdentifier(), String.class));

		Long ttl = entity.getTimeToLiveAccessor().getTimeToLive(source);
		if (ttl != null && ttl > 0) {
			sink.setTimeToLive(ttl);
		}

		for (IndexedData indexeData : indexResolver.resolveIndexesFor(entity.getTypeInformation(), source)) {
			sink.addIndexedData(indexeData);
		}

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

			if (customConversions.getCustomWriteTarget(value.getClass()).equals(byte[].class)) {
				sink.getBucket().put(StringUtils.hasText(path) ? path : "_raw", conversionService.convert(value, byte[].class));
			} else {
				writeToBucket(path, value, sink);
			}
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
					writeInternal(keyspace, propertyStringPath, accessor.getProperty(persistentProperty),
							persistentProperty.getTypeInformation().getActualType(), sink);
				} else {

					Object propertyValue = accessor.getProperty(persistentProperty);
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
	private void writeCollection(String keyspace, String path, Collection<?> values, TypeInformation<?> typeHint,
			RedisData sink) {

		if (values == null) {
			return;
		}

		int i = 0;
		for (Object value : values) {

			String currentPath = path + ".[" + i + "]";

			if (customConversions.hasCustomWriteTarget(value.getClass())) {
				writeToBucket(currentPath, value, sink);
			} else {
				writeInternal(keyspace, currentPath, value, typeHint, sink);
			}
			i++;
		}
	}

	private void writeToBucket(String path, Object value, RedisData sink) {

		if (value == null) {
			return;
		}

		if (customConversions.hasCustomWriteTarget(value.getClass())) {
			Class<?> targetType = customConversions.getCustomWriteTarget(value.getClass());

			if (ClassUtils.isAssignable(Map.class, targetType)) {

				Map<?, ?> map = (Map<?, ?>) conversionService.convert(value, targetType);
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					sink.getBucket().put(path + (StringUtils.hasText(path) ? "." : "") + entry.getKey(),
							toBytes(entry.getValue()));
				}
			} else if (ClassUtils.isAssignable(byte[].class, targetType)) {
				sink.getBucket().put(path, toBytes(value));
			} else {
				throw new IllegalArgumentException("converter must not fool me!!");
			}

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

		Bucket partial = source.getBucket().extract(path + ".[");

		List<String> keys = new ArrayList<String>(partial.keySet());
		keys.sort(listKeyComparator);

		Collection<Object> target = CollectionFactory.createCollection(collectionType, valueType, partial.size());

		for (String key : keys) {
			target.add(fromBytes(partial.get(key), valueType));
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

		List<String> keys = new ArrayList<String>(source.extractAllKeysFor(path));
		keys.sort(listKeyComparator);

		Collection<Object> target = CollectionFactory.createCollection(collectionType, valueType, keys.size());

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

			if (customConversions.hasCustomWriteTarget(entry.getValue().getClass())) {
				writeToBucket(currentPath, entry.getValue(), sink);
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

		Set<String> keys = source.getBucket().extractAllKeysFor(path);

		Map<Object, Object> target = CollectionFactory.createMap(mapType, keys.size());

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

}
