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

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.geo.Point;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.core.index.ConfigurableIndexDefinitionProvider;
import org.springframework.data.redis.core.index.GeoIndexDefinition;
import org.springframework.data.redis.core.index.GeoIndexed;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.IndexDefinition;
import org.springframework.data.redis.core.index.IndexDefinition.Condition;
import org.springframework.data.redis.core.index.IndexDefinition.IndexingContext;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.SimpleIndexDefinition;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.core.mapping.RedisPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * {@link IndexResolver} implementation considering properties annotated with {@link Indexed} or paths set up in
 * {@link IndexConfiguration}.
 *
 * @author Christoph Strobl
 * @author Greg Turnquist
 * @since 1.7
 */
public class PathIndexResolver implements IndexResolver {

	private final Set<Class<?>> VALUE_TYPES = new HashSet<>(Arrays.<Class<?>> asList(Point.class, GeoLocation.class));

	private final ConfigurableIndexDefinitionProvider indexConfiguration;
	private final RedisMappingContext mappingContext;
	private final IndexedDataFactoryProvider indexedDataFactoryProvider;

	/**
	 * Creates new {@link PathIndexResolver} with empty {@link IndexConfiguration}.
	 */
	public PathIndexResolver() {
		this(new RedisMappingContext());
	}

	/**
	 * Creates new {@link PathIndexResolver} with given {@link IndexConfiguration}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 */
	public PathIndexResolver(RedisMappingContext mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null!");

		this.mappingContext = mappingContext;
		this.indexConfiguration = mappingContext.getMappingConfiguration().getIndexConfiguration();
		this.indexedDataFactoryProvider = new IndexedDataFactoryProvider();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(org.springframework.data.util.TypeInformation, java.lang.Object)
	 */
	public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, @Nullable Object value) {
		return doResolveIndexesFor(mappingContext.getRequiredPersistentEntity(typeInformation).getKeySpace(), "",
				typeInformation, null, value);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(java.lang.String, java.lang.String, org.springframework.data.util.TypeInformation, java.lang.Object)
	 */
	@Override
	public Set<IndexedData> resolveIndexesFor(String keyspace, String path, TypeInformation<?> typeInformation,
			Object value) {
		return doResolveIndexesFor(keyspace, path, typeInformation, null, value);
	}

	private Set<IndexedData> doResolveIndexesFor(final String keyspace, final String path,
			TypeInformation<?> typeInformation, @Nullable PersistentProperty<?> fallback, @Nullable Object value) {

		RedisPersistentEntity<?> entity = mappingContext.getPersistentEntity(typeInformation);

		if (entity == null || (value != null && VALUE_TYPES.contains(value.getClass()))) {
			return resolveIndex(keyspace, path, fallback, value);
		}

		// this might happen on update where we address a property within an entity directly
		if (!ClassUtils.isAssignable(entity.getType(), value.getClass())) {

			String propertyName = path.lastIndexOf('.') > 0 ? path.substring(path.lastIndexOf('.') + 1, path.length()) : path;
			return resolveIndex(keyspace, path, entity.getPersistentProperty(propertyName), value);
		}

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);
		final Set<IndexedData> indexes = new LinkedHashSet<>();

		entity.doWithProperties(new PropertyHandler<RedisPersistentProperty>() {

			@Override
			public void doWithPersistentProperty(RedisPersistentProperty persistentProperty) {

				String currentPath = !path.isEmpty() ? path + "." + persistentProperty.getName() : persistentProperty.getName();

				Object propertyValue = accessor.getProperty(persistentProperty);

				if (propertyValue == null) {
					return;
				}

				TypeInformation<?> typeHint = persistentProperty.isMap()
						? persistentProperty.getTypeInformation().getRequiredMapValueType()
						: persistentProperty.getTypeInformation().getActualType();

				if (persistentProperty.isMap()) {

					for (Entry<?, ?> entry : ((Map<?, ?>) propertyValue).entrySet()) {

						TypeInformation<?> typeToUse = updateTypeHintForActualValue(typeHint, entry.getValue());
						indexes.addAll(doResolveIndexesFor(keyspace, currentPath + "." + entry.getKey(), typeToUse.getActualType(),
								persistentProperty, entry.getValue()));
					}

				} else if (persistentProperty.isCollectionLike()) {

					final Iterable<?> iterable;

					if (Iterable.class.isAssignableFrom(propertyValue.getClass())) {
						iterable = (Iterable<?>) propertyValue;
					} else if (propertyValue.getClass().isArray()) {
						iterable = CollectionUtils.arrayToList(propertyValue);
					} else {
						throw new RuntimeException("Don't know how to handle " + propertyValue.getClass() + " type of collection");
					}

					for (Object listValue : iterable) {

						if (listValue != null) {
							TypeInformation<?> typeToUse = updateTypeHintForActualValue(typeHint, listValue);
							indexes.addAll(
									doResolveIndexesFor(keyspace, currentPath, typeToUse.getActualType(), persistentProperty, listValue));
						}
					}
				}

				else if (persistentProperty.isEntity()
						|| persistentProperty.getTypeInformation().getActualType().equals(ClassTypeInformation.OBJECT)) {

					typeHint = updateTypeHintForActualValue(typeHint, propertyValue);
					indexes.addAll(
							doResolveIndexesFor(keyspace, currentPath, typeHint.getActualType(), persistentProperty, propertyValue));
				} else {
					indexes.addAll(resolveIndex(keyspace, currentPath, persistentProperty, propertyValue));
				}

			}

			private TypeInformation<?> updateTypeHintForActualValue(TypeInformation<?> typeHint, Object propertyValue) {

				if (typeHint.equals(ClassTypeInformation.OBJECT) || typeHint.getClass().isInterface()) {
					try {
						typeHint = mappingContext.getRequiredPersistentEntity(propertyValue.getClass()).getTypeInformation();
					} catch (Exception e) {
						// ignore for cases where property value cannot be resolved as an entity, in that case the provided type
						// hint has to be sufficient
					}
				}
				return typeHint;
			}

		});

		return indexes;
	}

	protected Set<IndexedData> resolveIndex(String keyspace, String propertyPath,
			@Nullable PersistentProperty<?> property, @Nullable Object value) {

		String path = normalizeIndexPath(propertyPath, property);

		Set<IndexedData> data = new LinkedHashSet<>();

		if (indexConfiguration.hasIndexFor(keyspace, path)) {

			IndexingContext context = new IndexingContext(keyspace, path,
					property != null ? property.getTypeInformation() : ClassTypeInformation.OBJECT);

			for (IndexDefinition indexDefinition : indexConfiguration.getIndexDefinitionsFor(keyspace, path)) {

				if (!verifyConditions(indexDefinition.getConditions(), value, context)) {
					continue;
				}

				Object transformedValue = indexDefinition.valueTransformer().convert(value);

				IndexedData indexedData = null;
				if (transformedValue == null) {
					indexedData = new RemoveIndexedData(indexedData);
				} else {
					indexedData = indexedDataFactoryProvider.getIndexedDataFactory(indexDefinition).createIndexedDataFor(value);
				}
				data.add(indexedData);
			}
		}

		else if (property != null && property.isAnnotationPresent(Indexed.class)) {

			SimpleIndexDefinition indexDefinition = new SimpleIndexDefinition(keyspace, path);
			indexConfiguration.addIndexDefinition(indexDefinition);

			data.add(indexedDataFactoryProvider.getIndexedDataFactory(indexDefinition).createIndexedDataFor(value));
		} else if (property != null && property.isAnnotationPresent(GeoIndexed.class)) {

			GeoIndexDefinition indexDefinition = new GeoIndexDefinition(keyspace, path);
			indexConfiguration.addIndexDefinition(indexDefinition);

			data.add(indexedDataFactoryProvider.getIndexedDataFactory(indexDefinition).createIndexedDataFor(value));
		}

		return data;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private boolean verifyConditions(Iterable<Condition<?>> conditions, Object value, IndexingContext context) {

		for (Condition condition : conditions) {

			// TODO: generics lookup
			if (!condition.matches(value, context)) {
				return false;
			}
		}

		return true;
	}

	private String normalizeIndexPath(String path, @Nullable PersistentProperty<?> property) {

		if (property == null) {
			return path;
		}

		if (property.isMap()) {
			return path.replaceAll("\\[", "").replaceAll("\\]", "");
		}
		if (property.isCollectionLike()) {
			return path.replaceAll("\\[(\\p{Digit})*\\]", "").replaceAll("\\.\\.", ".");
		}

		return path;
	}
}
