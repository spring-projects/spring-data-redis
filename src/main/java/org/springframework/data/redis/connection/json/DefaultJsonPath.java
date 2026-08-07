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

import org.springframework.util.Assert;

/**
 * Default implementation of {@link JsonPath}.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 */
record DefaultJsonPath(String path) implements JsonPath {

	static final DefaultJsonPath ROOT = new DefaultJsonPath("$");

	DefaultJsonPath {
		Assert.hasText(path, "JsonPath must not be empty");
		Assert.isTrue(path.charAt(0) == '$', "JsonPath must start with '$'");
	}

	@Override
	public String asString() {
		return path;
	}

	@Override
	public String toString() {
		return path;
	}

}
