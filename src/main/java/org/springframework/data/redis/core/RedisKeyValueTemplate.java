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
package org.springframework.data.redis.core;

import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueCallback;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.redis.core.mapping.RedisMappingContext;

/**
 * Redis specific implementation of {@link KeyValueTemplate}.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisKeyValueTemplate extends KeyValueTemplate {

	/**
	 * Create new {@link RedisKeyValueTemplate}.
	 * 
	 * @param adapter must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public RedisKeyValueTemplate(RedisKeyValueAdapter adapter, RedisMappingContext mappingContext) {
		super(adapter, mappingContext);
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
