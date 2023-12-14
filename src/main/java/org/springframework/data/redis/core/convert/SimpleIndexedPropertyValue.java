/*
 * Copyright 2015-2023 the original author or authors.
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
 * {@link IndexedData} implementation indicating storage of data within a Redis Set.
 *
 * @author Christoph Strobl
 * @author Rob Winch
 * @author Junghoon Ban
 * @since 1.7
 */
public record SimpleIndexedPropertyValue(String keyspace, String indexName, Object value) implements IndexedData {

	@Override
	public String toString() {
		return "SimpleIndexedPropertyValue{" + "keyspace='" + keyspace + '\'' + ", indexName='" + indexName + '\''
				+ ", value=" + value + '}';
	}
}
