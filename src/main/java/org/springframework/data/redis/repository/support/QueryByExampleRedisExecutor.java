/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.redis.repository.support;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.convert.DtoInstantiatingConverter;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.convert.IndexResolver;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.repository.query.ExampleQueryMapper;
import org.springframework.data.redis.repository.query.RedisOperationChain;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Repository fragment implementing Redis {@link QueryByExampleExecutor Query-by-Example} operations.
 * <p>
 * This executor uses {@link ExampleQueryMapper} to map {@link Example}s into {@link KeyValueQuery} to execute its query
 * methods.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
@SuppressWarnings("unchecked")
public class QueryByExampleRedisExecutor<T>
		implements QueryByExampleExecutor<T>, BeanFactoryAware, BeanClassLoaderAware {

	private final EntityInformation<T, ?> entityInformation;
	private final RedisKeyValueTemplate keyValueTemplate;
	private final ExampleQueryMapper mapper;
	private final SpelAwareProxyProjectionFactory projectionFactory;
	private final EntityInstantiators entityInstantiators = new EntityInstantiators();

	/**
	 * Create a new {@link QueryByExampleRedisExecutor} given {@link EntityInformation} and {@link RedisKeyValueTemplate}.
	 * This constructor uses the configured {@link IndexResolver} from the converter.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param keyValueTemplate must not be {@literal null}.
	 */
	public QueryByExampleRedisExecutor(EntityInformation<T, ?> entityInformation,
			RedisKeyValueTemplate keyValueTemplate) {

		this(entityInformation, keyValueTemplate,
				keyValueTemplate.getConverter().getIndexResolver() != null ? keyValueTemplate.getConverter().getIndexResolver()
						: new PathIndexResolver(keyValueTemplate.getMappingContext()));
	}

	/**
	 * Create a new {@link QueryByExampleRedisExecutor} given {@link EntityInformation} and {@link RedisKeyValueTemplate}.
	 *
	 * @param entityInformation must not be {@literal null}.
	 * @param keyValueTemplate must not be {@literal null}.
	 */
	public QueryByExampleRedisExecutor(EntityInformation<T, ?> entityInformation, RedisKeyValueTemplate keyValueTemplate,
			IndexResolver indexResolver) {

		Assert.notNull(entityInformation, "EntityInformation must not be null!");
		Assert.notNull(keyValueTemplate, "RedisKeyValueTemplate must not be null!");
		Assert.notNull(indexResolver, "IndexResolver must not be null!");

		this.entityInformation = entityInformation;
		this.keyValueTemplate = keyValueTemplate;

		this.mapper = new ExampleQueryMapper(keyValueTemplate.getMappingContext(), indexResolver);
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.projectionFactory.setBeanFactory(beanFactory);
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.projectionFactory.setBeanClassLoader(classLoader);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findOne(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> Optional<S> findOne(Example<S> example) {

		return Optional.ofNullable(doFindOne(example));
	}

	@Nullable
	private <S extends T> S doFindOne(Example<S> example) {

		Iterator<S> iterator = doFind(example);

		if (iterator.hasNext()) {
			S result = iterator.next();
			if (iterator.hasNext()) {
				throw new IncorrectResultSizeDataAccessException(1);
			}

			return result;
		}

		return null;
	}

	private <S extends T> Iterator<S> doFind(Example<S> example) {

		RedisOperationChain operationChain = createQuery(example);

		KeyValueQuery<RedisOperationChain> query = new KeyValueQuery<>(operationChain);
		return (Iterator<S>) keyValueTemplate.find(query.limit(2), entityInformation.getJavaType()).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> Iterable<S> findAll(Example<S> example) {

		RedisOperationChain operationChain = createQuery(example);

		return (Iterable<S>) keyValueTemplate.find(new KeyValueQuery<>(operationChain), entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example, org.springframework.data.domain.Sort)
	 */
	@Override
	public <S extends T> Iterable<S> findAll(Example<S> example, Sort sort) {
		throw new UnsupportedOperationException("Ordering is not supported");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example, org.springframework.data.domain.Pageable)
	 */
	@Override
	public <S extends T> Page<S> findAll(Example<S> example, Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be null!");

		RedisOperationChain operationChain = createQuery(example);

		KeyValueQuery<RedisOperationChain> query = new KeyValueQuery<>(operationChain);
		List<S> result = (List<S>) keyValueTemplate.find(
				query.orderBy(pageable.getSort()).skip(pageable.getOffset()).limit(pageable.getPageSize()),
				entityInformation.getJavaType());

		return PageableExecutionUtils.getPage(result, pageable,
				() -> operationChain.isEmpty() ? keyValueTemplate.count(entityInformation.getJavaType())
						: keyValueTemplate.count(query, entityInformation.getJavaType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#count(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> long count(Example<S> example) {

		RedisOperationChain operationChain = createQuery(example);

		return keyValueTemplate.count(new KeyValueQuery<>(operationChain), entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#exists(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> boolean exists(Example<S> example) {
		return count(example) > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findBy(org.springframework.data.domain.Example, java.util.function.Function)
	 */
	@Override
	public <S extends T, R> R findBy(Example<S> example,
			Function<org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery<S>, R> queryFunction) {

		Assert.notNull(example, "Example must not be null!");
		Assert.notNull(queryFunction, "Query function must not be null!");

		return queryFunction.apply(new FluentQueryByExample<>(example, example.getProbeType()));
	}

	private <S extends T> RedisOperationChain createQuery(Example<S> example) {

		Assert.notNull(example, "Example must not be null!");

		return mapper.getMappedExample(example);
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery} using {@link Example}.
	 *
	 * @author Mark Paluch
	 * @since 2.6
	 */
	class FluentQueryByExample<S extends T, R> implements FluentQuery.FetchableFluentQuery<R> {

		private final Example<S> example;
		private final Sort sort;
		private final Class<?> domainType;
		private final Class<R> resultType;

		FluentQueryByExample(Example<S> example, Class<R> resultType) {
			this(example, Sort.unsorted(), resultType, resultType);
		}

		FluentQueryByExample(Example<S> example, Sort sort, Class<?> domainType, Class<R> resultType) {
			this.example = example;
			this.sort = sort;
			this.domainType = domainType;
			this.resultType = resultType;
		}

		@Override
		public FetchableFluentQuery<R> sortBy(Sort sort) {
			return new FluentQueryByExample<>(example, sort, domainType, resultType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#as(java.lang.Class)
		 */
		@Override
		public <R1> FetchableFluentQuery<R1> as(Class<R1> resultType) {
			return new FluentQueryByExample<>(example, sort, domainType, resultType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#project(java.util.Collection)
		 */
		@Override
		public FetchableFluentQuery<R> project(Collection<String> properties) {
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#oneValue()
		 */
		@Nullable
		@Override
		public R oneValue() {

			S one = doFindOne(example);

			if (one != null) {
				return getConversionFunction(entityInformation.getJavaType(), resultType).apply(one);
			}

			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#firstValue()
		 */
		@Nullable
		@Override
		public R firstValue() {

			Iterator<S> iterator = doFind(example);

			if (iterator.hasNext()) {
				return getConversionFunction(entityInformation.getJavaType(), resultType).apply(iterator.next());
			}

			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#all()
		 */
		@Override
		public List<R> all() {
			return stream().collect(Collectors.toList());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#page(org.springframework.data.domain.Pageable)
		 */
		@Override
		public Page<R> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null!");

			Function<Object, R> conversionFunction = getConversionFunction(entityInformation.getJavaType(), resultType);

			List<R> content = findAll(example, pageable).stream().map(conversionFunction).collect(Collectors.toList());
			return PageableExecutionUtils.getPage(content, pageable, this::count);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#stream()
		 */
		@Override
		public Stream<R> stream() {

			Function<Object, R> conversionFunction = getConversionFunction(entityInformation.getJavaType(), resultType);

			if (sort.isSorted()) {
				return findAll(example, PageRequest.of(0, Integer.MAX_VALUE, sort)).stream().map(conversionFunction);
			}

			return Streamable.of(findAll(example)).map(conversionFunction).stream();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#count()
		 */
		@Override
		public long count() {
			return QueryByExampleRedisExecutor.this.count(example);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery#exists()
		 */
		@Override
		public boolean exists() {
			return QueryByExampleRedisExecutor.this.exists(example);
		}

		private <P> Function<Object, P> getConversionFunction(Class<?> inputType, Class<P> targetType) {

			if (targetType.isAssignableFrom(inputType)) {
				return (Function<Object, P>) Function.identity();
			}

			if (targetType.isInterface()) {
				return o -> projectionFactory.createProjection(targetType, o);
			}

			DtoInstantiatingConverter converter = new DtoInstantiatingConverter(targetType,
					keyValueTemplate.getMappingContext(), entityInstantiators);

			return o -> (P) converter.convert(o);
		}
	}
}
