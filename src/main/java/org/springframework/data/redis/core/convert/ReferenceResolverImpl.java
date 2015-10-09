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

import java.io.Serializable;

import org.springframework.data.redis.core.RedisKeyValueAdapter;

/**
 * Default {@link ReferenceResolver} implementation;
 * 
 * @author Christoph Strobl
 */
public class ReferenceResolverImpl implements ReferenceResolver {

	private RedisKeyValueAdapter adapter;

	ReferenceResolverImpl() {

	}

	/**
	 * @param adapter must not be {@literal null}.
	 */
	public ReferenceResolverImpl(RedisKeyValueAdapter adapter) {

		// Assert.notNull(adapter, "KeyValueAdapter must not be null!");
		this.adapter = adapter;
	}

	public void setAdapter(RedisKeyValueAdapter adapter) {
		this.adapter = adapter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.ReferenceResolver#resolveReference(java.io.Serializable, java.io.Serializable, java.lang.Class)
	 */
	@Override
	public <T> T resolveReference(Serializable id, Serializable keyspace, Class<T> type) {
		return (T) adapter.get(id, keyspace);
	}
}
