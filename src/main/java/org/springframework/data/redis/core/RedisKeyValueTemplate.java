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
package org.springframework.data.redis.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueCallback;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Redis specific implementation of {@link KeyValueTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisKeyValueTemplate extends KeyValueTemplate {

	private final RedisKeyValueAdapter adapter;

	/**
	 * Create new {@link RedisKeyValueTemplate}.
	 *
	 * @param adapter must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public RedisKeyValueTemplate(RedisKeyValueAdapter adapter, RedisMappingContext mappingContext) {
		super(adapter, mappingContext);
		this.adapter = adapter;
	}

	/**
	 * Obtain the underlying redis specific {@link org.springframework.data.convert.EntityConverter}.
	 *
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	public RedisConverter getConverter() {
		return adapter.getConverter();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueTemplate#getMappingContext()
	 */
	@Override
	public RedisMappingContext getMappingContext() {
		return (RedisMappingContext) super.getMappingContext();
	}

	/**
	 * Retrieve entities by resolving their {@literal id}s and converting them into required type. <br />
	 * The callback provides either a single {@literal id} or an {@link Iterable} of {@literal id}s, used for retrieving
	 * the actual domain types and shortcuts manual retrieval and conversion of {@literal id}s via {@link RedisTemplate}.
	 *
	 * <pre>
	 * <code>
	 * List&#60;RedisSession&#62; sessions = template.find(new RedisCallback&#60;Set&#60;byte[]&#62;&#62;() {
	 *   public Set&#60;byte[]&#60; doInRedis(RedisConnection connection) throws DataAccessException {
	 *     return connection
	 *       .sMembers("spring:session:sessions:securityContext.authentication.principal.username:user"
	 *         .getBytes());
	 *   }
	 * }, RedisSession.class);
	 * </code>
	 *
	 * <pre>
	 *
	 * @param callback provides the to retrieve entity ids. Must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @return empty list if not elements found.
	 */
	public <T> List<T> find(RedisCallback<?> callback, Class<T> type) {

		Assert.notNull(callback, "Callback must not be null.");

		return execute(new RedisKeyValueCallback<List<T>>() {

			@Override
			public List<T> doInRedis(RedisKeyValueAdapter adapter) {

				Object callbackResult = adapter.execute(callback);

				if (callbackResult == null) {
					return Collections.emptyList();
				}

				Iterable<?> ids = ClassUtils.isAssignable(Iterable.class, callbackResult.getClass())
						? (Iterable<?>) callbackResult : Collections.singleton(callbackResult);

				List<T> result = new ArrayList<>();
				for (Object id : ids) {

					String idToUse = adapter.getConverter().getConversionService().canConvert(id.getClass(), String.class)
							? adapter.getConverter().getConversionService().convert(id, String.class) : id.toString();

					findById(idToUse, type).ifPresent(result::add);
				}

				return result;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueTemplate#insert(java.lang.Object, java.lang.Object)
	 */
	@Override
	public <T> T insert(Object id, T objectToInsert) {

		if (objectToInsert instanceof PartialUpdate) {
			doPartialUpdate((PartialUpdate<?>) objectToInsert);
			return objectToInsert;
		}

		if (!(objectToInsert instanceof RedisData)) {

			RedisConverter converter = adapter.getConverter();

			RedisPersistentEntity<?> entity = converter.getMappingContext()
					.getRequiredPersistentEntity(objectToInsert.getClass());

			KeyValuePersistentProperty idProperty = entity.getRequiredIdProperty();
			PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(objectToInsert);

			if (propertyAccessor.getProperty(idProperty) == null) {

				propertyAccessor.setProperty(idProperty, id);
				return super.insert(id, propertyAccessor.getBean());
			}
		}

		return super.insert(id, objectToInsert);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueTemplate#update(java.lang.Object)
	 */
	@Override
	public <T> T update(T objectToUpdate) {

		if (objectToUpdate instanceof PartialUpdate) {
			doPartialUpdate((PartialUpdate<?>) objectToUpdate);

			return objectToUpdate;
		}

		return super.update(objectToUpdate);
	}

	@Override
	public <T> T update(Object id, T objectToUpdate) {
		return super.update(id, objectToUpdate);
	}

	protected void doPartialUpdate(final PartialUpdate<?> update) {

		execute(new RedisKeyValueCallback<Void>() {

			@Override
			public Void doInRedis(RedisKeyValueAdapter adapter) {

				adapter.update(update);
				return null;
			}
		});
	}

	/**
	 * Redis specific {@link KeyValueCallback}.
	 *
	 * @author Christoph Strobl
	 * @param <T>
	 * @since 1.7
	 */
	public static abstract class RedisKeyValueCallback<T> implements KeyValueCallback<T> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.keyvalue.core.KeyValueCallback#doInKeyValue(org.springframework.data.keyvalue.core.KeyValueAdapter)
		 */
		@Override
		public T doInKeyValue(KeyValueAdapter adapter) {
			return doInRedis((RedisKeyValueAdapter) adapter);
		}

		public abstract T doInRedis(RedisKeyValueAdapter adapter);
	}

}
