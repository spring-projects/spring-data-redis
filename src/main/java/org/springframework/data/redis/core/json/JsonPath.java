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
package org.springframework.data.redis.core.json;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable value object representing a JSONPath expression for use with Redis JSON operations.
 * <p>
 * Provides a fluent API to construct JSONPath expressions in a type-safe manner.
 * Use {@link #root()} as a starting point for path construction.
 *
 * <h2>Examples</h2>
 * <pre class="code">
 * JsonPath.root().child("address").child("city");  // $.address.city
 * JsonPath.root().child("items").index(0);         // $.items[0]
 * JsonPath.root().child("items").last();           // $.items[-1]
 * JsonPath.root().child("store").wildcard();       // $.store.*
 * JsonPath.root().recursive().child("price");      // $..price
 * JsonPath.root().child("items").slice(1, 4);      // $.items[1:4]
 * JsonPath.root().child("items")
 *     .filter(FilterExpression.of("@.price &lt; 10")); // $.items[?(@.price &lt; 10)]
 * </pre>
 *
 * @author Yordan Tsintsov
 * @see <a href="https://redis.io/docs/latest/develop/data-types/json/path/">Redis JSONPath Documentation</a>
 * @since 4.3
 */
@NullMarked
public final class JsonPath {

	private static final String ROOT_PATH = "$";
	private static final JsonPath ROOT = new JsonPath(ROOT_PATH);

	private final String path;

	private JsonPath(String path) {
		this.path = path;
	}

	/**
	 * Returns the root path {@code $}.
	 *
	 * @return the root {@link JsonPath}
	 */
	public static JsonPath root() {
		return ROOT;
	}

	/**
	 * Creates a {@link JsonPath} from a raw path string.
	 *
	 * @param path the path expression, must not be empty
	 * @return a new {@link JsonPath}
	 */
	public static JsonPath of(String path) {

		Assert.hasText(path, "Path must not be empty");

		return new JsonPath(path);
	}

	/**
	 * Appends a child property access ({@code .child}).
	 *
	 * @param child the property name, must not be empty
	 * @return a new {@link JsonPath}
	 */
	public JsonPath child(String child) {

		Assert.hasText(child, "Child must not be empty");

		return new JsonPath(path + "." + child);
	}

	/**
	 * Appends an array index access ({@code [index]}).
	 *
	 * @param index the array index (negative values count from end)
	 * @return a new {@link JsonPath}
	 */
	public JsonPath index(int index) {
		return new JsonPath(path + "[" + index + "]");
	}

	/**
	 * Appends access to the first array element ({@code [0]}).
	 *
	 * @return a new {@link JsonPath}
	 */
	public JsonPath first() {
		return new JsonPath(path + "[0]");
	}

	/**
	 * Appends access to the last array element ({@code [-1]}).
	 *
	 * @return a new {@link JsonPath}
	 */
	public JsonPath last() {
		return new JsonPath(path + "[-1]");
	}

	/**
	 * Appends recursive descent ({@code ..}).
	 *
	 * @return a new {@link JsonPath}
	 */
	public JsonPath recursive() {
		return new JsonPath(path + "..");
	}

	/**
	 * Appends a wildcard for all properties ({@code .*}).
	 *
	 * @return a new {@link JsonPath}
	 */
	public JsonPath wildcard() {
		return new JsonPath(path + ".*");
	}

	/**
	 * Appends a wildcard for all array elements ({@code [*]}).
	 *
	 * @return a new {@link JsonPath}
	 */
	public JsonPath allElements() {
		return new JsonPath(path + "[*]");
	}

	/**
	 * Appends an array slice from {@code start} to end ({@code [start:]}).
	 *
	 * @param start the start index (inclusive)
	 * @return a new {@link JsonPath}
	 */
	public JsonPath slice(int start) {
		return new JsonPath(path + "[" + start + ":]");
	}

	/**
	 * Appends an array slice ({@code [start:end]}).
	 *
	 * @param start the start index (inclusive)
	 * @param end the end index (exclusive)
	 * @return a new {@link JsonPath}
	 */
	public JsonPath slice(int start, int end) {
		return new JsonPath(path + "[" + start + ":" + end + "]");
	}

	/**
	 * Appends an array slice with step ({@code [start:end:step]}).
	 *
	 * @param start the start index (inclusive)
	 * @param end the end index (exclusive)
	 * @param step the step value, must not be zero
	 * @return a new {@link JsonPath}
	 */
	public JsonPath slice(int start, int end, int step) {

		Assert.isTrue(step != 0, "Step must not be zero");

		return new JsonPath(path + "[" + start + ":" + end + ":" + step + "]");
	}

	/**
	 * Appends access to multiple array indices ({@code [0,2,4]}).
	 *
	 * @param indices the array indices
	 * @return a new {@link JsonPath}
	 */
	public JsonPath indices(int... indices) {

		Assert.isTrue(indices.length > 0, "Indices must not be empty");

		String joined = Arrays.stream(indices)
				.mapToObj(String::valueOf)
				.collect(Collectors.joining(","));

		return new JsonPath(path + "[" + joined + "]");
	}

	/**
	 * Appends access to multiple named children ({@code ['a','b']}).
	 *
	 * @param names the property names
	 * @return a new {@link JsonPath}
	 */
	public JsonPath children(String... names) {

		Assert.notEmpty(names, "Names must not be empty");

		String joined = Arrays.stream(names)
				.map(n -> "'" + n + "'")
				.collect(Collectors.joining(","));

		return new JsonPath(path + "[" + joined + "]");
	}

	/**
	 * Returns the path as a string.
	 *
	 * @return the path expression
	 */
	public String getPath() {
		return path;
	}

	/**
	 * Appends a filter expression ({@code [?(expression)]}).
	 *
	 * @param filter the filter expression
	 * @return a new {@link JsonPath}
	 */
	public JsonPath filter(FilterExpression filter) {
		return new JsonPath(path + filter);
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JsonPath jsonPath = (JsonPath) o;
		return Objects.equals(path, jsonPath.path);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(path);
	}

	@Override
	public String toString() {
		return path;
	}

	/**
	 * A filter expression for JSONPath queries (e.g., {@code @.price < 10}).
	 */
	public static final class FilterExpression {

		private final String expression;

		private FilterExpression(String expression) {
			this.expression = expression;
		}

		/**
		 * Creates a filter expression from the given string.
		 *
		 * @param expression the filter expression (e.g., {@code "@.price < 10"})
		 * @return a new {@link FilterExpression}
		 */
		public static FilterExpression of(String expression) {

			Assert.hasText(expression, "Expression must not be empty");

			return new FilterExpression(expression);
		}

		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			FilterExpression that = (FilterExpression) o;
			return Objects.equals(expression, that.expression);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(expression);
		}

		@Override
		public String toString() {
			return "[?(" + expression + ")]";
		}

	}

}
