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

import org.springframework.util.ObjectUtils;

/**
 * {@link IndexedData} implementation indicating storage of data within a Redis Set.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class SimpleIndexedPropertyValue implements IndexedData {

	private final String keyspace;
	private final String indexName;
	private final Object value;

	/**
	 * Creates new {@link SimpleIndexedPropertyValue}.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param indexName must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	public SimpleIndexedPropertyValue(String keyspace, String indexName, Object value) {

		this.keyspace = keyspace;
		this.indexName = indexName;
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
	public String getIndexName() {
		return indexName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getKeySpace()
	 */
	@Override
	public String getKeySpace() {
		return this.keyspace;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 1;
		result += ObjectUtils.nullSafeHashCode(keyspace);
		result += ObjectUtils.nullSafeHashCode(indexName);
		result += ObjectUtils.nullSafeHashCode(value);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof SimpleIndexedPropertyValue)) {
			return false;
		}

		SimpleIndexedPropertyValue that = (SimpleIndexedPropertyValue) obj;

		if (!ObjectUtils.nullSafeEquals(this.keyspace, that.keyspace)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(this.indexName, that.indexName)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(this.value, that.value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SimpleIndexedPropertyValue [keyspace=" + keyspace + ", indexName=" + indexName + ", value=" + value + "]";
	}

}
