/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.redis.test.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import org.springframework.util.ObjectUtils;

/**
 * Generic comparator that dives into {@link java.util.Collections} and uses
 * {@link ObjectUtils#nullSafeEquals(Object, Object)}.
 *
 * @author Mark Paluch
 */
public enum CollectionAwareComparator implements Comparator<Object> {

	INSTANCE;

	@Override
	public int compare(Object o1, Object o2) {

		if (o1 instanceof Collection && o2 instanceof Collection) {

			Collection<?> c1 = (Collection<?>) o1;
			Collection<?> c2 = (Collection<?>) o2;

			if (c1.size() != c2.size()) {
				return 1;
			}

			Iterator<?> i1 = c1.iterator();
			Iterator<?> i2 = c2.iterator();

			while (i1.hasNext()) {
				int result = compare(i1.next(), i2.next());
				if (result != 0) {
					return result;
				}
			}

			return 0;
		}

		if (!ObjectUtils.nullSafeEquals(o1, o2)) {
			return 1;
		}

		return 0;
	}
}
