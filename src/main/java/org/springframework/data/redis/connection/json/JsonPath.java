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
package org.springframework.data.redis.connection.json;

/**
 * JSON path abstraction for JSON commands.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public interface JsonPath {

	/**
	 * Root JSON path.
	 *
	 * @return {@link JsonPath} representing root JSON path.
	 */
	static JsonPath root() {
		return DefaultJsonPath.ROOT;
	}

	/**
	 * JSON path from a {@code String}.
	 *
	 * @param path the JSON path. Must not be {@literal null}.
	 * @return {@link JsonPath} representing JSON path.
	 */
	static JsonPath of(String path) {
		return new DefaultJsonPath(path);
	}

	/**
	 * Returns the canonical JSON path text of this value.
	 *
	 * @return the canonical JSON path text of this value.
	 */
	String asString();

}
