/*
 * Copyright 2011-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import org.springframework.lang.Nullable;

/**
 * Entity containing the parameters for the SORT operation.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface SortParameters {

	/**
	 * Sorting order.
	 */
	enum Order {
		ASC, DESC
	}

	/**
	 * Utility class wrapping the 'LIMIT' setting.
	 */
	class Range {

		private final long start;
		private final long count;

		public Range(long start, long count) {
			this.start = start;
			this.count = count;
		}

		public long getStart() {
			return start;
		}

		public long getCount() {
			return count;
		}
	}

	/**
	 * Returns the sorting order. Can be null if nothing is specified.
	 *
	 * @return sorting order. {@literal null} if not set.
	 */
	@Nullable
	Order getOrder();

	/**
	 * Indicates if the sorting is numeric (default) or alphabetical (lexicographical). Can be null if nothing is
	 * specified.
	 *
	 * @return the type of sorting. {@literal null} if not set.
	 */
	@Nullable
	Boolean isAlphabetic();

	/**
	 * Returns the pattern (if set) for sorting by external keys (<tt>BY</tt>). Can be null if nothing is specified.
	 *
	 * @return <tt>BY</tt> pattern. {@literal null} if not set.
	 */
	@Nullable
	byte[] getByPattern();

	/**
	 * Returns the pattern (if set) for retrieving external keys (<tt>GET</tt>). Can be null if nothing is specified.
	 *
	 * @return <tt>GET</tt> pattern. {@literal null} if not set.
	 */
	@Nullable
	byte[][] getGetPattern();

	/**
	 * Returns the sorting limit (range or pagination). Can be null if nothing is specified.
	 *
	 * @return sorting limit/range. {@literal null} if not set.
	 */
	@Nullable
	Range getLimit();
}
