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
 * Value abstraction for JSON payloads passed to {@code RedisJsonCommands}.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public interface JsonValue {

	/**
	 * JSON {@code null} value.
	 *
	 * @return {@link JsonValue} representing JSON {@code null}.
	 */
	static JsonValue nullValue() {
		return DefaultJsonValue.NULL;
	}

	/**
	 * JSON boolean from a {@code boolean}.
	 *
	 * @param value the boolean value.
	 * @return {@link JsonValue} representing JSON boolean.
	 */
	static JsonValue of(boolean value) {
		return new DefaultJsonValue(Boolean.toString(value));
	}

	/**
	 * JSON number from a {@link Number}.
	 *
	 * @param number must not be {@literal null}.
	 * @return {@link JsonValue} representing JSON number.
	 */
	static JsonValue of(Number number) {
		return new DefaultJsonValue(number.toString());
	}

	/**
	 * JSON number from an {@code int}.
	 *
	 * @param number the value.
	 * @return {@link JsonValue} representing JSON number.
	 */
	static JsonValue of(int number) {
		return new DefaultJsonValue(Integer.toString(number));
	}

	/**
	 * JSON number from a {@code long}.
	 *
	 * @param number the value.
	 * @return {@link JsonValue} representing JSON number.
	 */
	static JsonValue of(long number) {
		return new DefaultJsonValue(Long.toString(number));
	}

	/**
	 * JSON number from a {@code double}.
	 *
	 * @param number the value; must be finite.
	 * @return {@link JsonValue} representing JSON number.
	 */
	static JsonValue of(double number) {
		return new DefaultJsonValue(Double.toString(number));
	}

	/**
	 * JSON string from a Java {@link String}.
	 *
	 * @param value must not be {@literal null}.
	 * @return {@link JsonValue} representing JSON string.
	 */
	static JsonValue of(String value) {
		return new DefaultJsonValue(DefaultJsonValue.quote(value));
	}

	/**
	 * JSON value from a JSON document. The supplied text is forwarded unchanged; no validation or escaping is
	 * performed.
	 *
	 * @param json a valid JSON document. Must not be {@literal null}.
	 * @return a {@link JsonValue} carrying the JSON.
	 */
	static JsonValue raw(String json) {
		return new DefaultJsonValue(json);
	}

	/**
	 * @return the canonical JSON text of this value.
	 */
	String asString();

}
