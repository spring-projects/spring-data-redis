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

/**
 * Simple builder class for constructing {@link SortQuery}.
 *
 * @author Costin Leau
 */
public class SortQueryBuilder<K> extends DefaultSortCriterion<K> {

	private static final String NO_SORT_KEY = "~";

	private SortQueryBuilder(K key) {
		super(key);
	}

	public static <K> SortQueryBuilder<K> sort(K key) {
		return new SortQueryBuilder<>(key);
	}

	public SortCriterion<K> by(String keyPattern) {
		return addBy(keyPattern);
	}

	public SortCriterion<K> noSort() {
		return by(NO_SORT_KEY);
	}
}
