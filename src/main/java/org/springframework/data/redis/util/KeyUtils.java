/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.util;

import java.util.Arrays;

/**
 * Utility class for Redis keys.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public abstract class KeyUtils {

	// ---------------------------------------------------------------------
	// General convenience methods for working with keys
	// ---------------------------------------------------------------------

	/**
	 * Utility method to split an array concatenated of keys into the first key and the remaining keys and invoke the
	 * given function.
	 *
	 * @param keys array of keys to be separated into the first one and the remaining ones.
	 * @param function function to be invoked with the first and remaining keys as input arguments.
	 * @return result of the {@link SourceKeysFunction}.
	 */
	public static <T, R> R splitKeys(T[] keys, SourceKeysFunction<T, R> function) {

		if (keys.length == 0) {
			throw new IllegalArgumentException("Keys array must contain at least one element");
		}

		T firstKey = keys[0];
		T[] otherKeys = Arrays.copyOfRange(keys, 1, keys.length);
		return function.apply(firstKey, otherKeys);
	}

	/**
	 * Represents a function that accepts two arguments of the same base type and produces a result while the second
	 * argument is an array of {@code T}. This is similar to a {@link java.util.function.BiFunction} but restricts both
	 * arguments to be of the same type. Typically used in arrangements where a composite collection of keys is split into
	 * the first key and the remaining keys.
	 * <p>
	 * This is a <a href="package-summary.html">functional interface</a> whose functional method is
	 * {@link #apply(Object, Object[])}.
	 *
	 * @param <T> the type of the first argument to the function.
	 * @param <R> the type of the result of the function.
	 */
	public interface SourceKeysFunction<T, R> {

		/**
		 * Applies this function to the given arguments.
		 *
		 * @param firstKey the first key argument.
		 * @param otherKeys the other keys function argument.
		 * @return the function result.
		 */
		R apply(T firstKey, T[] otherKeys);

	}

	// utility constructor
	private KeyUtils() {

	}

}
