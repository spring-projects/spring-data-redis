/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.core.convert;

/**
 * {@link IndexedData} represents a secondary index for a property path in a given keyspace.
 *
 * @author Christoph Strobl
 * @author Rob Winch
 * @since 1.7
 */
public interface IndexedData {

	/**
	 * Get the {@link String} representation of the index name.
	 *
	 * @return never {@literal null}.
	 */
	String getIndexName();

	/**
	 * Get the associated keyspace the index resides in.
	 *
	 * @return
	 */
	String getKeyspace();

	/**
	 * Return the key prefix for usage in Redis.
	 *
	 * @return concatenated form of the keyspace and the index name.
	 * @since 3.3.4
	 */
	default String getKeyPrefix() {
		return getKeyspace() + ":" + getIndexName();
	}

}
