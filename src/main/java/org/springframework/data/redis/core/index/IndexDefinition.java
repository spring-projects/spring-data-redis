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
package org.springframework.data.redis.core.index;

import lombok.Value;

import java.util.Collection;

import org.springframework.data.util.TypeInformation;

/**
 * {@link IndexDefinition} allow to set up a blueprint for creating secondary index structures in Redis. Setting up
 * conditions allows to define {@link Condition} that have to be passed in order to add a value to the index. This
 * allows to fine grained tune the index structure. {@link IndexValueTransformer} gets applied to the raw value for
 * creating the actual index entry.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public interface IndexDefinition {

	/**
	 * @return never {@literal null}.
	 */
	String getKeyspace();

	/**
	 * @return never {@literal null}.
	 */
	Collection<Condition<?>> getConditions();

	/**
	 * @return never {@literal null}.
	 */
	IndexValueTransformer valueTransformer();

	/**
	 * @return never {@literal null}.
	 */
	String getIndexName();

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 * @param <T>
	 */
	interface Condition<T> {
		boolean matches(T value, IndexingContext context);
	}

	/**
	 * Context in which a particular value is about to get indexed.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	@Value
	public class IndexingContext {

		private final String keyspace;
		private final String path;
		private final TypeInformation<?> typeInformation;
	}
}
