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

import java.io.Serializable;
import java.util.Set;

/**
 * {@link IndexDefinitionProvider} give access to {@link IndexDefinition}s for creating secondary index structures.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public interface IndexDefinitionProvider {

	/**
	 * Checks if an index is defined for a given {@code keyspace}.
	 *
	 * @param keyspace the keyspace to get
	 * @return never {@literal null}
	 */
	boolean hasIndexFor(Serializable keyspace);

	/**
	 * Checks if an index is defined for a given {@code keyspace} and property {@code path}.
	 *
	 * @param keyspace
	 * @param path
	 * @return true if index is defined.
	 */
	boolean hasIndexFor(Serializable keyspace, String path);

	/**
	 * Get the list of {@link IndexDefinition} for a given {@code keyspace}.
	 *
	 * @param keyspace
	 * @return never {@literal null}.
	 */
	Set<IndexDefinition> getIndexDefinitionsFor(Serializable keyspace);

	/**
	 * Get the list of {@link IndexDefinition} for a given {@code keyspace} and property {@code path}.
	 *
	 * @param keyspace
	 * @param path
	 * @return never {@literal null}.
	 */
	Set<IndexDefinition> getIndexDefinitionsFor(Serializable keyspace, String path);
}
