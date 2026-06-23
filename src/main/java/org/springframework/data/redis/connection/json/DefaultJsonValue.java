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
 * Default implementation of {@link JsonValue}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
record DefaultJsonValue(String value) implements JsonValue {

	static final DefaultJsonValue NULL = new DefaultJsonValue("null");

	DefaultJsonValue {
		Assert.notNull(value, "Value must not be null");
	}

	static String quote(String value) {
		Assert.notNull(value, "Value must not be null");

		StringBuilder sb = new StringBuilder(value.length() + 2);
		sb.append('"');

		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			switch (c) {
				case '"'  -> sb.append("\\\"");
				case '\\' -> sb.append("\\\\");
				default -> {
					if (c < 0x20) {
						sb.append(String.format("\\u%04x", (int) c));
					} else {
						sb.append(c);
					}
				}
			}
		}

		sb.append('"');
		return sb.toString();
	}

	@Override
	public String asString() {
		return value;
	}

}
