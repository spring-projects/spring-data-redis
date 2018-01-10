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
package org.springframework.data.redis.repository.support;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.convert.IndexResolver;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.repository.query.ExampleQueryMapper;
import org.springframework.data.redis.repository.query.RedisOperationChain;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.util.Assert;

/**
 * Repository fragment implementing Redis {@link QueryByExampleExecutor Query-by-Example} operations.
 * <p/>
 * This executor uses {@link ExampleQueryMapper} to map {@link Example}s into {@link KeyValueQuery} to execute its query
 * methods.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.1
 */
@SuppressWarnings("unchecked")
public class QueryByExampleRedisExecutor<T> implements QueryByExampleExecutor<T> {

	private final EntityInformation<T, ?> entityInformation;
	private final RedisKeyValueTemplate keyValueTemplate;
	private final ExampleQueryMapper mapper;

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
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findOne(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> Optional<S> findOne(Example<S> example) {

		RedisOperationChain operationChain = createQuery(example);

		KeyValueQuery<RedisOperationChain> query = new KeyValueQuery<>(operationChain);
		Iterator<T> iterator = keyValueTemplate.find(query.limit(2), entityInformation.getJavaType()).iterator();

		Optional result = Optional.empty();

		if (iterator.hasNext()) {
			result = Optional.of((S) iterator.next());
			if (iterator.hasNext()) {
				throw new IncorrectResultSizeDataAccessException(1);
			}
		}

		return result;
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
		Iterable<T> result = keyValueTemplate.find(
				query.orderBy(pageable.getSort()).skip(pageable.getOffset()).limit(pageable.getPageSize()),
				entityInformation.getJavaType());

		long count = operationChain.isEmpty() ? keyValueTemplate.count(entityInformation.getJavaType())
				: keyValueTemplate.count(query, entityInformation.getJavaType());

		List<S> list = new ArrayList<>();
		for (T t : result) {
			list.add((S) t);
		}

		return new PageImpl<>(list, pageable, count);
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

	private <S extends T> RedisOperationChain createQuery(Example<S> example) {

		Assert.notNull(example, "Example must not be null!");

		return mapper.getMappedExample(example);
	}
}
