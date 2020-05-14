/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import org.springframework.data.geo.Point;
import org.springframework.util.ObjectUtils;

/**
 * {@link IndexedData} implementation indicating storage of data within a Redis GEO structure.
 *
 * @author Christoph Strobl
 * @since 1.8
 */
public class GeoIndexedPropertyValue implements IndexedData {

	private final String keyspace;
	private final String indexName;
	private final Point value;

	public GeoIndexedPropertyValue(String keyspace, String indexName, Point value) {

		this.keyspace = keyspace;
		this.indexName = indexName;
		this.value = value;
	}

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

	public Point getValue() {
		return this.value;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof GeoIndexedPropertyValue)) {
			return false;
		}

		GeoIndexedPropertyValue that = (GeoIndexedPropertyValue) o;
		if (!ObjectUtils.nullSafeEquals(keyspace, that.keyspace)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(indexName, that.indexName)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(value, that.value);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(keyspace);
		result = 31 * result + ObjectUtils.nullSafeHashCode(indexName);
		result = 31 * result + ObjectUtils.nullSafeHashCode(value);
		return result;
	}

	protected boolean canEqual(Object other) {
		return other instanceof GeoIndexedPropertyValue;
	}

	public String toString() {
		return "GeoIndexedPropertyValue(keyspace=" + this.getKeyspace() + ", indexName=" + this.getIndexName() + ", value="
				+ this.getValue() + ")";
	}
}
