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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.springframework.util.Assert;

/**
 * Default implementation of {@link JsonValue}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
final class DefaultJsonValue implements JsonValue {

	private static final byte[] NULL_BYTES = "null".getBytes(StandardCharsets.UTF_8);

	static final DefaultJsonValue NULL = new DefaultJsonValue(NULL_BYTES);

	private final byte[] value;

	DefaultJsonValue(byte[] value) {
		Assert.notNull(value, "Value must not be null");
		this.value = value;
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
		return new String(value, StandardCharsets.UTF_8);
	}

	@Override
	public boolean equals(Object obj) {

		if (obj == this) {
			return true;
		}
		if (!(obj instanceof DefaultJsonValue that)) {
			return false;
		}
		return Arrays.equals(this.value, that.value);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(value);
	}

	@Override
	public String toString() {
		return asString();
	}

}
