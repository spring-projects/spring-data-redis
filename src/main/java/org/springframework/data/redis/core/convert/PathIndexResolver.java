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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.redis.core.index.ConfigurableIndexDefinitionProvider;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.IndexDefinition;
import org.springframework.data.redis.core.index.IndexDefinition.Condition;
import org.springframework.data.redis.core.index.IndexDefinition.IndexingContext;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.SimpleIndexDefinition;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * {@link IndexResolver} implementation considering properties annotated with {@link Indexed} or paths set up in
 * {@link IndexConfiguration}.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class PathIndexResolver implements IndexResolver {

	private ConfigurableIndexDefinitionProvider indexConfiguration;
	private RedisMappingContext mappingContext;

	/**
	 * Creates new {@link PathIndexResolver} with empty {@link IndexConfiguration}.
	 */
	public PathIndexResolver() {
		this(new RedisMappingContext());
	}

	/**
	 * Creates new {@link PathIndexResolver} with given {@link IndexConfiguration}.
	 *
	 * @param mapppingContext must not be {@literal null}.
	 */
	public PathIndexResolver(RedisMappingContext mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null!");
		this.mappingContext = mappingContext;
		this.indexConfiguration = mappingContext.getMappingConfiguration().getIndexConfiguration();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(org.springframework.data.util.TypeInformation, java.lang.Object)
	 */
	public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, Object value) {
		return doResolveIndexesFor(mappingContext.getPersistentEntity(typeInformation).getKeySpace(), "", typeInformation,
				value);
	}

	private Set<IndexedData> doResolveIndexesFor(final String keyspace, final String path,
			TypeInformation<?> typeInformation, Object value) {

		RedisPersistentEntity<?> entity = mappingContext.getPersistentEntity(typeInformation);

		if (entity == null) {
			return resolveIndex(keyspace, path, null, value);
		}

		final PersistentPropertyAccessor accessor = entity.getPropertyAccessor(value);
		final Set<IndexedData> indexes = new LinkedHashSet<IndexedData>();

		entity.doWithProperties(new PropertyHandler<KeyValuePersistentProperty>() {

			@Override
			public void doWithPersistentProperty(KeyValuePersistentProperty persistentProperty) {

				String currentPath = !path.isEmpty() ? path + "." + persistentProperty.getName() : persistentProperty.getName();

				Object propertyValue = accessor.getProperty(persistentProperty);

				if (propertyValue != null) {

					TypeInformation<?> typeHint = persistentProperty.isMap() ? persistentProperty.getTypeInformation()
							.getMapValueType() : persistentProperty.getTypeInformation().getActualType();

					indexes.addAll(resolveIndex(keyspace, currentPath, persistentProperty, propertyValue));

					if (persistentProperty.isMap()) {

						for (Entry<?, ?> entry : ((Map<?, ?>) propertyValue).entrySet()) {

							TypeInformation<?> typeToUse = updateTypeHintForActualValue(typeHint, entry.getValue());
							indexes.addAll(doResolveIndexesFor(keyspace, currentPath + "." + entry.getKey(),
									typeToUse.getActualType(), entry.getValue()));
						}

					} else if (persistentProperty.isCollectionLike()) {

						for (Object listValue : (Iterable<?>) propertyValue) {

							TypeInformation<?> typeToUse = updateTypeHintForActualValue(typeHint, listValue);
							indexes.addAll(doResolveIndexesFor(keyspace, currentPath, typeToUse.getActualType(), listValue));
						}
					}

					else if (persistentProperty.isEntity()
							|| persistentProperty.getTypeInformation().getActualType().equals(ClassTypeInformation.OBJECT)) {

						typeHint = updateTypeHintForActualValue(typeHint, propertyValue);
						indexes.addAll(doResolveIndexesFor(keyspace, currentPath, typeHint.getActualType(), propertyValue));
					}
				}

			}

			private TypeInformation<?> updateTypeHintForActualValue(TypeInformation<?> typeHint, Object propertyValue) {

				if (typeHint.equals(ClassTypeInformation.OBJECT) || typeHint.getClass().isInterface()) {
					try {
						typeHint = mappingContext.getPersistentEntity(propertyValue.getClass()).getTypeInformation();
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

	protected Set<IndexedData> resolveIndex(String keyspace, String propertyPath, PersistentProperty<?> property,
			Object value) {

		if (value == null) {
			return Collections.emptySet();
		}

		String path = normalizeIndexPath(propertyPath, property);

		Set<IndexedData> data = new LinkedHashSet<IndexedData>();

		if (indexConfiguration.hasIndexFor(keyspace, path)) {

			IndexingContext context = new IndexingContext(keyspace, path, property != null ? property.getTypeInformation()
					: ClassTypeInformation.OBJECT);

			for (IndexDefinition indexDefinition : indexConfiguration.getIndexDefinitionsFor(keyspace, path)) {

				if (!verifyConditions(indexDefinition.getConditions(), value, context)) {
					continue;
				}

				data.add(new SimpleIndexedPropertyValue(keyspace, indexDefinition.getIndexName(), indexDefinition
						.valueTransformer().convert(value)));
			}
		}

		else if (property != null && property.isAnnotationPresent(Indexed.class)) {

			SimpleIndexDefinition indexDefinition = new SimpleIndexDefinition(keyspace, path);
			indexConfiguration.addIndexDefinition(indexDefinition);

			data.add(new SimpleIndexedPropertyValue(keyspace, path, indexDefinition.valueTransformer().convert(value)));
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

	private String normalizeIndexPath(String path, PersistentProperty<?> property) {

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
