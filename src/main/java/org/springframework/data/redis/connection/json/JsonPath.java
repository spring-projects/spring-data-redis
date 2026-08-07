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
 * <p>
 * Supports RedisJSON 2.0 Path syntax only. The syntax below is adapted from
 * <a href="https://goessner.net/articles/JsonPath/">Goessner's path syntax comparison</a>.
 * <p>
 * Given the JSON document:
 *
 * <pre class="code">
 * {
 *   "store": {
 *     "books": [
 *       { "title": "Sayings of the Century", "price": 8.95 },
 *       { "title": "Sword of Honour", "price": 12.99 }
 *     ]
 *   }
 * }
 * </pre>
 *
 * the following paths can be used to navigate the document:
 *
 * <pre class="code">
 * $.store.books[0].title                 // "Sayings of the Century"
 * $.store.books[*].price                 // [8.95, 12.99]
 * $.store.books[?(@.price&lt;10)].title  // "Sayings of the Century"
 * $..price                               // [8.95, 12.99]
 * </pre>
 * <table>
 * <caption>JSONPath syntax elements</caption>
 * <tr>
 * <th>Syntax element</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>{@code $}</td>
 * <td>The root (outermost JSON element), starts the path.</td>
 * </tr>
 * <tr>
 * <td>{@code .} or {@code []}</td>
 * <td>Selects a child element.</td>
 * </tr>
 * <tr>
 * <td>{@code ..}</td>
 * <td>Recursively descends through the JSON document.</td>
 * </tr>
 * <tr>
 * <td>{@code *}</td>
 * <td>Wildcard, returns all elements.</td>
 * </tr>
 * <tr>
 * <td>{@code []}</td>
 * <td>Subscript operator, accesses an array element.</td>
 * </tr>
 * <tr>
 * <td>{@code [,]}</td>
 * <td>Union, selects multiple elements.</td>
 * </tr>
 * <tr>
 * <td>{@code [start:end:step]}</td>
 * <td>Array slice where {@code start}, {@code end}, and {@code step} are index values. Values can be omitted from the
 * slice (for example, {@code [3:]}, {@code [:8:2]}) to use the default values: {@code start} defaults to the first
 * index, {@code end} defaults to the last index, and {@code step} defaults to {@code 1}. Use {@code [*]} or {@code [:]}
 * to select all elements.</td>
 * </tr>
 * <tr>
 * <td>{@code ?()}</td>
 * <td>Filters a JSON object or array. Supports comparison operators ({@code ==}, {@code !=}, {@code <}, {@code <=},
 * {@code >}, {@code >=}, {@code =~}), logical operators ({@code &&}, {@code ||}, {@code !}), arithmetic operators
 * ({@code +}, {@code -}, {@code *}, {@code /}, {@code %}), membership operators ({@code in}, {@code nin}), set-relation
 * operators ({@code subsetof}, {@code anyof}, {@code noneof}), the {@code size}/{@code sizeof} and {@code empty}
 * operators, and parentheses ({@code (}, {@code )}).</td>
 * </tr>
 * <tr>
 * <td>{@code ()}</td>
 * <td>Script expression.</td>
 * </tr>
 * <tr>
 * <td>{@code @}</td>
 * <td>The current element, used in filter or script expressions.</td>
 * </tr>
 * <tr>
 * <td>{@code ~}</td>
 * <td>Returns the names of an object's members as a list of strings.</td>
 * </tr>
 * </table>
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.2
 * @see <a href="https://redis.io/docs/latest/develop/data-types/json/path/">RedisJSON Path</a>
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
	 * JSON path from a {@code String}. The path must start with {@code $} to represent the root according to the v2.0
	 * path semantics.
	 *
	 * @param path the JSON path.
	 * @return {@link JsonPath} representing JSON path.
	 */
	static JsonPath raw(String path) {
		return new DefaultJsonPath(path);
	}

	/**
	 * Returns the canonical JSON path text of this value.
	 *
	 * @return the canonical JSON path text of this value.
	 */
	String asString();

}
