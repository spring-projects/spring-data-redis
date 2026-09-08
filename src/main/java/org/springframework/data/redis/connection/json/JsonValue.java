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
 * A JSON value for use with {@link org.springframework.data.redis.connection.RedisJsonCommands}.
 * <p>
 * Use the {@code of} factory methods to convert Java scalar values to JSON. Use {@link #raw(String)} or
 * {@link #raw(byte[])} for an existing JSON representation. Raw values are accepted without validation.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @author Moritz Halbritter
 * @since 4.2
 */
public interface JsonValue {

	/**
	 * Return a value representing JSON {@literal null}.
	 *
	 * @return the JSON null value.
	 */
	static JsonValue nullValue() {
		return DefaultJsonValue.NULL;
	}

	/**
	 * Create a JSON boolean from the given value.
	 *
	 * @param value the boolean value.
	 * @return the JSON boolean value.
	 */
	static JsonValue of(boolean value) {
		return new DefaultJsonValue(Boolean.toString(value));
	}

	/**
	 * Create a JSON number from the given {@link Number}.
	 *
	 * @param number the number to represent.
	 * @return the JSON number value.
	 */
	static JsonValue of(Number number) {
		if (number instanceof Double) {
			return of((double) number);
		}

		if (number instanceof Float) {
			return of((float) number);
		}
		return new DefaultJsonValue(number.toString());
	}

	/**
	 * Create a JSON number from the given {@code int}.
	 *
	 * @param number the number to represent.
	 * @return the JSON number value.
	 */
	static JsonValue of(int number) {
		return new DefaultJsonValue(Integer.toString(number));
	}

	/**
	 * Create a JSON number from the given {@code long}.
	 *
	 * @param number the number to represent.
	 * @return the JSON number value.
	 */
	static JsonValue of(long number) {
		return new DefaultJsonValue(Long.toString(number));
	}

	/**
	 * Create a JSON number from the given {@code float}.
	 *
	 * @param number the number to represent, must be finite.
	 * @return the JSON number value.
	 * @throws IllegalArgumentException if the number is not finite.
	 */
	static JsonValue of(float number) {
		Assert.isTrue(Float.isFinite(number), "Float value must be finite");
		return new DefaultJsonValue(Float.toString(number));
	}

	/**
	 * Create a JSON number from the given {@code double}.
	 *
	 * @param number the number to represent, must be finite.
	 * @return the JSON number value.
	 * @throws IllegalArgumentException if the number is not finite.
	 */
	static JsonValue of(double number) {
		Assert.isTrue(Double.isFinite(number), "Double value must be finite");
		return new DefaultJsonValue(Double.toString(number));
	}

	/**
	 * Create a JSON string from the given Java string.
	 * <p>
	 * The value is quoted and escaped as required by JSON string syntax.
	 *
	 * @param value the string to represent.
	 * @return the JSON string value.
	 * @see #raw(String)
	 */
	static JsonValue of(String value) {
		return new DefaultJsonValue(DefaultJsonValue.quote(value));
	}

	/**
	 * Create a value from the given JSON bytes.
	 * <p>
	 * The bytes are used without validation or conversion.
	 *
	 * @param json a valid JSON value encoded as UTF-8.
	 * @return the JSON value.
	 */
	static JsonValue raw(byte[] json) {
		Assert.notNull(json, "JSON must not be null");
		return new DefaultJsonValue(json);
	}

	/**
	 * Create a value from the given JSON string.
	 * <p>
	 * The string is encoded as UTF-8 without validation, quoting, or escaping.
	 *
	 * @param json a valid JSON value.
	 * @return the JSON value.
	 * @see #of(String)
	 */
	static JsonValue raw(String json) {
		Assert.notNull(json, "JSON must not be null");
		return new DefaultJsonValue(json);
	}

	/**
	 * Return the JSON representation of this value as UTF-8 bytes.
	 *
	 * @return the JSON bytes, including the literal {@code null} for {@link #nullValue()}.
	 */
	byte[] asBytes();

	/**
	 * Return the JSON representation of this value as a string.
	 *
	 * @return the JSON string, including {@code "null"} for {@link #nullValue()}.
	 */
	String asString();

}
