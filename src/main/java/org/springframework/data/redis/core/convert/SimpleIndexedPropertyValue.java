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

/**
 * {@link IndexedData} implementation indicating storage of data within a Redis Set.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class SimpleIndexedPropertyValue implements IndexedData {

	private final String keyspace;
	private final String path;
	private final Object value;

	/**
	 * Creates new {@link SimpleIndexedPropertyValue}.
	 * 
	 * @param keyspace must not be {@literal null}.
	 * @param path must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	public SimpleIndexedPropertyValue(String keyspace, String path, Object value) {

		this.keyspace = keyspace;
		this.path = path;
		this.value = value;
	}

	/**
	 * Get the value to index.
	 * 
	 * @return can be {@literal null}.
	 */
	public Object getValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getPath()
	 */
	@Override
	public String getPath() {
		return path;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getKeySpace()
	 */
	@Override
	public String getKeySpace() {
		return this.keyspace;
	}

}
