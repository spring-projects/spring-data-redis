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
package org.springframework.data.redis.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.query.KeyValuePartTreeQuery;
import org.springframework.data.keyvalue.repository.query.KeyValuePartTreeQuery.QueryInitialization;
import org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.repository.core.MappingRedisEntityInformation;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.util.Assert;

/**
 * {@link RepositoryFactorySupport} specific of handing Redis
 * {@link org.springframework.data.keyvalue.repository.KeyValueRepository}.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisRepositoryFactory extends KeyValueRepositoryFactory {

	/**
	 * @param keyValueOperations
	 * @see KeyValueRepositoryFactory#KeyValueRepositoryFactory(KeyValueOperations)
	 */
	public RedisRepositoryFactory(KeyValueOperations keyValueOperations) {
		super(keyValueOperations);
	}

	/**
	 * @param keyValueOperations
	 * @param queryCreator
	 * @see KeyValueRepositoryFactory#KeyValueRepositoryFactory(KeyValueOperations, Class)
	 */
	public RedisRepositoryFactory(KeyValueOperations keyValueOperations,
			Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
		super(keyValueOperations, queryCreator);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactory#getQueryLookupStrategy(org.springframework.data.repository.query.QueryLookupStrategy.Key, org.springframework.data.repository.query.EvaluationContextProvider)
	 */
	@Override
	protected QueryLookupStrategy getQueryLookupStrategy(Key key, EvaluationContextProvider evaluationContextProvider) {
		return new RedisQueryLookupStrategy(key, evaluationContextProvider, getKeyValueOperations(), getQueryCreator());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactory#getEntityInformation(java.lang.Class)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T, ID extends Serializable> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

		RedisPersistentEntity<T> entity = (RedisPersistentEntity<T>) getMappingContext().getPersistentEntity(domainClass);
		EntityInformation<T, ID> entityInformation = (EntityInformation<T, ID>) new MappingRedisEntityInformation<T, ID>(
				entity);

		return entityInformation;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	private static class RedisQueryLookupStrategy implements QueryLookupStrategy {

		private EvaluationContextProvider evaluationContextProvider;
		private KeyValueOperations keyValueOperations;

		private Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

		/**
		 * Creates a new {@link RedisQueryLookupStrategy} for the given {@link Key}, {@link EvaluationContextProvider},
		 * {@link KeyValueOperations} and query creator type.
		 * <p>
		 * 
		 * @param key
		 * @param evaluationContextProvider must not be {@literal null}.
		 * @param keyValueOperations must not be {@literal null}.
		 * @param queryCreator must not be {@literal null}.
		 */
		public RedisQueryLookupStrategy(Key key, EvaluationContextProvider evaluationContextProvider,
				KeyValueOperations keyValueOperations, Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {

			Assert.notNull(evaluationContextProvider, "EvaluationContextProvider must not be null!");
			Assert.notNull(keyValueOperations, "KeyValueOperations must not be null!");
			Assert.notNull(queryCreator, "Query creator type must not be null!");

			this.evaluationContextProvider = evaluationContextProvider;
			this.keyValueOperations = keyValueOperations;
			this.queryCreator = queryCreator;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.repository.core.NamedQueries)
		 */
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			QueryMethod queryMethod = new QueryMethod(method, metadata, factory);
			KeyValuePartTreeQuery partTreeQuery = new KeyValuePartTreeQuery(queryMethod, evaluationContextProvider,
					this.keyValueOperations, this.queryCreator);
			partTreeQuery.setQueryIntialization(QueryInitialization.NEW);
			return partTreeQuery;
		}
	}
}
