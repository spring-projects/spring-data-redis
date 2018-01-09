/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.repository.query;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher.MatchMode;
import org.springframework.data.domain.ExampleMatcher.PropertyValueTransformer;
import org.springframework.data.domain.ExampleMatcher.StringMatcher;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.redis.core.convert.IndexResolver;
import org.springframework.data.redis.core.convert.IndexedData;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.core.mapping.RedisPersistentProperty;
import org.springframework.data.support.ExampleMatcherAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Mapper for Query-by-Example examples to an actual query.
 * <p/>
 * This mapper creates a {@link RedisOperationChain} for a given {@link Example} considering exact matches,
 * {@link PropertyValueTransformer value transformations} and {@link MatchMode} for indexed simple and nested type
 * properties. {@link java.util.Map} and {@link java.util.Collection} properties are not considered.
 * <p/>
 * Example matching is limited to case-sensitive and exact matches only.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class ExampleQueryMapper {

	private final Set<StringMatcher> SUPPORTED_MATCHERS = EnumSet.of(StringMatcher.DEFAULT, StringMatcher.EXACT);

	private final MappingContext<RedisPersistentEntity<?>, RedisPersistentProperty> mappingContext;
	private final IndexResolver indexResolver;

	/**
	 * Creates a new {@link ExampleQueryMapper} given {@link MappingContext} and {@link IndexResolver}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param indexResolver must not be {@literal null}.
	 */
	public ExampleQueryMapper(MappingContext<RedisPersistentEntity<?>, RedisPersistentProperty> mappingContext,
			IndexResolver indexResolver) {

		Assert.notNull(mappingContext, "MappingContext must not be null!");
		Assert.notNull(indexResolver, "IndexResolver must not be null!");

		this.mappingContext = mappingContext;
		this.indexResolver = indexResolver;
	}

	/**
	 * Retrieve a mapped {@link RedisOperationChain} to query secondary indexes given {@link Example}.
	 *
	 * @param example must not be {@literal null}.
	 * @return the mapped {@link RedisOperationChain}.
	 */
	public RedisOperationChain getMappedExample(Example<?> example) {

		RedisOperationChain chain = new RedisOperationChain();

		ExampleMatcherAccessor matcherAccessor = new ExampleMatcherAccessor(example.getMatcher());

		applyPropertySpecs("", example.getProbe(), mappingContext.getRequiredPersistentEntity(example.getProbeType()),
				matcherAccessor, example.getMatcher().getMatchMode(), chain);

		return chain;
	}

	private void applyPropertySpecs(String path, @Nullable Object probe, RedisPersistentEntity<?> persistentEntity,
			ExampleMatcherAccessor exampleSpecAccessor, MatchMode matchMode, RedisOperationChain chain) {

		if (probe == null) {
			return;
		}

		PersistentPropertyAccessor propertyAccessor = persistentEntity.getPropertyAccessor(probe);

		Set<IndexedData> indexedData = getIndexedData(path, probe, persistentEntity);
		Set<String> indexNames = indexedData.stream().map(IndexedData::getIndexName).distinct().collect(Collectors.toSet());

		persistentEntity.forEach(property -> {

			if (property.isIdProperty()) {
				return;
			}

			String propertyPath = StringUtils.hasText(path) ? path + "." + property.getName() : property.getName();

			if (exampleSpecAccessor.isIgnoredPath(propertyPath) || property.isCollectionLike() || property.isMap()) {
				return;
			}

			applyPropertySpec(propertyPath, indexNames::contains, exampleSpecAccessor, propertyAccessor, property, matchMode,
					chain);
		});
	}

	private void applyPropertySpec(String path, Predicate<String> hasIndex, ExampleMatcherAccessor exampleSpecAccessor,
			PersistentPropertyAccessor propertyAccessor, RedisPersistentProperty property, MatchMode matchMode,
			RedisOperationChain chain) {

		StringMatcher stringMatcher = exampleSpecAccessor.getDefaultStringMatcher();
		boolean ignoreCase = exampleSpecAccessor.isIgnoreCaseEnabled();
		Object value = propertyAccessor.getProperty(property);

		if (exampleSpecAccessor.hasPropertySpecifiers()) {
			stringMatcher = exampleSpecAccessor.getStringMatcherForPath(path);
			ignoreCase = exampleSpecAccessor.isIgnoreCaseForPath(path);
		}

		if (ignoreCase) {
			throw new InvalidDataAccessApiUsageException("Redis Query-by-Example supports only case-sensitive matching.");
		}

		if (!SUPPORTED_MATCHERS.contains(stringMatcher)) {
			throw new InvalidDataAccessApiUsageException(
					String.format("Redis Query-by-Example does not support string matcher %s. Supported matchers are: %s.",
							stringMatcher, SUPPORTED_MATCHERS));
		}

		if (exampleSpecAccessor.hasPropertySpecifier(path)) {

			PropertyValueTransformer valueTransformer = exampleSpecAccessor.getValueTransformerForPath(path);
			value = valueTransformer.apply(Optional.ofNullable(value)).orElse(null);
		}

		if (value == null) {
			return;
		}

		if (property.isEntity()) {
			applyPropertySpecs(path, value, mappingContext.getRequiredPersistentEntity(property), exampleSpecAccessor,
					matchMode, chain);
		} else {

			if (matchMode == MatchMode.ALL) {
				if (hasIndex.test(path)) {
					chain.sismember(path, value);
				}
			} else {
				chain.orSismember(path, value);
			}
		}
	}

	private Set<IndexedData> getIndexedData(String path, Object probe, RedisPersistentEntity<?> persistentEntity) {

		String keySpace = persistentEntity.getKeySpace();
		return keySpace == null ? Collections.emptySet()
				: indexResolver.resolveIndexesFor(persistentEntity.getKeySpace(), path, persistentEntity.getTypeInformation(),
						probe);
	}
}
