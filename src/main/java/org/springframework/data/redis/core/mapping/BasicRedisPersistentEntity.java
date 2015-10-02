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
package org.springframework.data.redis.core.mapping;

import org.springframework.data.keyvalue.core.mapping.BasicKeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLiveResolver;
import org.springframework.data.util.TypeInformation;

/**
 * {@link RedisPersistentEntity} implementation.
 * 
 * @author Christoph Strobl
 * @param <T>
 */
public class BasicRedisPersistentEntity<T> extends BasicKeyValuePersistentEntity<T> implements RedisPersistentEntity<T> {

	private Long timeToLive;

	/**
	 * Creates new {@link BasicRedisPersistentEntity}.
	 * 
	 * @param information must not be {@literal null}.
	 * @param fallbackKeySpaceResolver can be {@literal null}.
	 * @param timeToLiveResolver can be {@literal null}.
	 */
	public BasicRedisPersistentEntity(TypeInformation<T> information, KeySpaceResolver fallbackKeySpaceResolver,
			TimeToLiveResolver timeToLiveResolver) {
		super(information, fallbackKeySpaceResolver);

		RedisHash hash = findAnnotation(RedisHash.class);
		if (hash != null && hash.timeToLive() > 0) {
			timeToLive = hash.timeToLive();
		} else if (timeToLiveResolver != null) {
			timeToLive = timeToLiveResolver.resolveTimeToLive(information.getType());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.mapping.RedisPersistentEntity#getTimeToLive()
	 */
	@Override
	public Long getTimeToLive() {
		return timeToLive;
	}
}
