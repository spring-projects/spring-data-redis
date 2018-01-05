/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.repository.core;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

/**
 * {@link RedisEntityInformation} implementation using a {@link RedisPersistentEntity} instance to lookup the necessary
 * information. Can be configured with a custom collection to be returned which will trump the one returned by the
 * {@link RedisPersistentEntity} if given.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T>
 * @param <ID>
 */
public class MappingRedisEntityInformation<T, ID> extends PersistentEntityInformation<T, ID>
		implements RedisEntityInformation<T, ID> {

	/**
	 * @param entity
	 */
	public MappingRedisEntityInformation(RedisPersistentEntity<T> entity) {

		super(entity);

		if (!entity.hasIdProperty()) {

			throw new MappingException(
					String.format("Entity %s requires to have an explicit id field. Did you forget to provide one using @Id?",
							entity.getName()));
		}
	}
}
