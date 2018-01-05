/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.core.convert;

/**
 * {@link RemoveIndexedData} represents a removed index entry from a secondary index for a property path in a given keyspace.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class RemoveIndexedData implements IndexedData {

	private final IndexedData delegate;

	RemoveIndexedData(IndexedData delegate) {
		super();
		this.delegate = delegate;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getIndexName()
	 */
	@Override
	public String getIndexName() {
		return delegate.getIndexName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getKeyspace()
	 */
	@Override
	public String getKeyspace() {
		return delegate.getKeyspace();
	}

	@Override
	public String toString() {
		return "RemoveIndexedData [indexName=" + getIndexName() + ", keyspace()=" + getKeyspace() + "]";
	}

}
