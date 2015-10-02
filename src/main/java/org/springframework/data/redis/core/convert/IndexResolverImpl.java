/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.RedisIndexDefinition;

/**
 * @author Christoph Strobl
 */
public class IndexResolverImpl implements IndexResolver {

	private IndexConfiguration indexConfiguration;

	public IndexResolverImpl() {

		this(new IndexConfiguration());
	}

	public IndexResolverImpl(IndexConfiguration indexConfiguration) {

		this.indexConfiguration = indexConfiguration != null ? indexConfiguration : new IndexConfiguration();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndex(java.lang.String, java.lang.String, org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty, java.lang.Object)
	 */
	@Override
	public IndexedData resolveIndex(String keyspace, String path, PersistentProperty<?> property, Object value) {

		if (indexConfiguration.hasIndexFor(keyspace, path)) {
			return new SimpleIndexedPropertyValue(keyspace, path, value);
		}

		else if (property.isAnnotationPresent(Indexed.class)) {

			Indexed indexed = property.findAnnotation(Indexed.class);

			indexConfiguration.addIndexDefinition(new RedisIndexDefinition(keyspace, path, indexed.type()));

			switch (indexed.type()) {
				case SIMPLE:
					return new SimpleIndexedPropertyValue(keyspace, path, value);
				default:
					throw new IllegalArgumentException(String.format("Unsupported index type '%s' for path '%s'.",
							indexed.type(), path));
			}
		}
		// TODO Auto-generated method stub
		return null;
	}
}
