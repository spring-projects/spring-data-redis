/*
 * Copyright 2021 the original author or authors.
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

import java.util.Iterator;

/**
 * Extension to {@link Iterator} that can be {@link #limit(long) limited} to a maximum number of items.
 *
 * @author Mark Paluch
 * @since 2.5
 */
public interface BoundedIterator<T> extends Iterator<T> {

	/**
	 * Limit the maximum number of elements to return. The limit is only applied to the returned instance and not applied
	 * to {@code this} iterator.
	 *
	 * @param count the maximum number of elements of iterator to return. Must be greater or equal to zero.
	 * @return a new instance of {@link BoundedIterator} with {@code count} applied.
	 */
	BoundedIterator<T> limit(long count);
}
