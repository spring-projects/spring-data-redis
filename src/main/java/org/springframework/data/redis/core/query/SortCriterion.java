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
package org.springframework.data.redis.core.query;

import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.SortParameters.Range;

/**
 * Internal interface part of the Sort DSL. Exposes generic operations.
 *
 * @author Costin Leau
 */
public interface SortCriterion<K> {

	SortCriterion<K> limit(long offset, long count);

	SortCriterion<K> limit(Range range);

	SortCriterion<K> order(Order order);

	SortCriterion<K> alphabetical(boolean alpha);

	SortCriterion<K> get(String pattern);

	SortQuery<K> build();
}
