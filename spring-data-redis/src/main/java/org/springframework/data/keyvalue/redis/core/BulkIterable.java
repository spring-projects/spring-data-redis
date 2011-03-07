/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

import java.util.Iterator;
import java.util.List;

/**
 * Wrapper class allowing for stream-like access across a list of values.
 * 
 * @author Costin Leau
 */
class BulkIterable<T> implements Iterable<T> {

	private final List<T> list;
	private volatile int index = 0;

	public BulkIterable(List<T> list) {
		this.list = list;
	}

	public boolean hasMore() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				return index < list.size();
			}

			@Override
			public T next() {
				return list.get(index++);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
