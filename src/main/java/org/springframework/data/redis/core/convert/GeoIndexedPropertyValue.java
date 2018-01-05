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

import org.springframework.data.geo.Point;

import lombok.Data;

/**
 * {@link IndexedData} implementation indicating storage of data within a Redis GEO structure.
 *
 * @author Christoph Strobl
 * @since 1.8
 */
@Data
public class GeoIndexedPropertyValue implements IndexedData {

	private final String keyspace;
	private final String indexName;
	private final Point value;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getIndexName()
	 */
	@Override
	public String getIndexName() {
		return GeoIndexedPropertyValue.geoIndexName(indexName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexedData#getKeyspace()
	 */
	@Override
	public String getKeyspace() {
		return keyspace;
	}

	public Point getPoint() {
		return value;
	}

	public static String geoIndexName(String path) {

		int index = path.lastIndexOf('.');
		if (index == -1) {
			return path;
		}
		StringBuilder sb = new StringBuilder(path);
		sb.setCharAt(index, ':');
		return sb.toString();
	}
}
