/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection;

import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.redis.search.DropIndexOptions;
import org.springframework.data.redis.search.IndexDefinition;
import org.springframework.data.redis.search.Schema;
import org.springframework.data.redis.search.SearchOptions;
import org.springframework.data.redis.search.SearchQuery;
import org.springframework.data.redis.search.SearchResult;

/**
 * Redis Search commands ({@code FT.*}) operating on a single connection.
 * <p>
 * These commands are not available in pipeline or transaction mode and will throw an exception if invoked in that context.
 *
 * @author Viktoriya Kutsarova
 * @see <a href="https://redis.io/docs/latest/develop/interact/search-and-query/">Redis Search</a>
 */
@NullUnmarked
public interface RedisSearchCommands {

	/**
	 * Create a new Redis Search index with default options.
	 *
	 * @param name index name; must not be {@literal null}.
	 * @param schema field definitions for the index; must not be {@literal null}.
	 * @return {@code "OK"} on success.
	 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE</a>
	 */
	default String createIndex(String name, Schema schema) {
		return createIndex(name, schema, IndexDefinition.create());
	}

	/**
	 * Create a new Redis Search index with the given definition.
	 *
	 * @param name index name; must not be {@literal null}.
	 * @param schema field definitions for the index; must not be {@literal null}.
	 * @param definition index-level options (data type, prefixes, language, etc.); must not be {@literal null}.
	 * @return {@code "OK"} on success.
	 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE</a>
	 */
	String createIndex(String name, Schema schema, IndexDefinition definition);

	/**
	 * Drop an existing index without deleting the underlying documents.
	 *
	 * @param name index name; must not be {@literal null}.
	 * @see <a href="https://redis.io/commands/ft.dropindex/">FT.DROPINDEX</a>
	 */
	default void dropIndex(String name) {
		dropIndex(name, DropIndexOptions.create());
	}

	/**
	 * Drop an existing index with the given options.
	 *
	 * @param name index name; must not be {@literal null}.
	 * @param options drop options; must not be {@literal null}.
	 * @see DropIndexOptions#deleteDocuments()
	 * @see <a href="https://redis.io/commands/ft.dropindex/">FT.DROPINDEX</a>
	 */
	void dropIndex(String name, DropIndexOptions options);

	/**
	 * Execute a full-text / field-range search against an index using default options.
	 *
	 * @param index index name; must not be {@literal null}.
	 * @param query search query; must not be {@literal null}.
	 * @return the search result; never {@literal null}.
	 * @see SearchQuery#of(String)
	 * @see SearchQuery#all()
	 * @see <a href="https://redis.io/commands/ft.search/">FT.SEARCH</a>
	 */
	default SearchResult search(String index, SearchQuery query) {
		return search(index, query, SearchOptions.create());
	}

	/**
	 * Execute a full-text / field-range search against an index with the given options.
	 *
	 * @param index index name; must not be {@literal null}.
	 * @param query search query; must not be {@literal null}.
	 * @param options search options (LIMIT, SORTBY, RETURN, etc.); must not be {@literal null}.
	 * @return the search result; never {@literal null}.
	 * @see SearchQuery#of(String)
	 * @see SearchQuery#all()
	 * @see <a href="https://redis.io/commands/ft.search/">FT.SEARCH</a>
	 */
	SearchResult search(String index, SearchQuery query, SearchOptions options);
}

