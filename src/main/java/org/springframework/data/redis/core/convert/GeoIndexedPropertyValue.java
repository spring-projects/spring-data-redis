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

	public boolean equals(final Object o) {
		if (o == this)
			return true;
		if (!(o instanceof GeoIndexedPropertyValue))
			return false;
		final GeoIndexedPropertyValue other = (GeoIndexedPropertyValue) o;
		if (!other.canEqual((Object) this))
			return false;
		final Object this$keyspace = this.getKeyspace();
		final Object other$keyspace = other.getKeyspace();
		if (this$keyspace == null ? other$keyspace != null : !this$keyspace.equals(other$keyspace))
			return false;
		final Object this$indexName = this.getIndexName();
		final Object other$indexName = other.getIndexName();
		if (this$indexName == null ? other$indexName != null : !this$indexName.equals(other$indexName))
			return false;
		final Object this$value = this.getValue();
		final Object other$value = other.getValue();
		if (this$value == null ? other$value != null : !this$value.equals(other$value))
			return false;
		return true;
	}

	protected boolean canEqual(final Object other) {
		return other instanceof GeoIndexedPropertyValue;
	}

	public int hashCode() {
		final int PRIME = 59;
		int result = 1;
		final Object $keyspace = this.getKeyspace();
		result = result * PRIME + ($keyspace == null ? 43 : $keyspace.hashCode());
		final Object $indexName = this.getIndexName();
		result = result * PRIME + ($indexName == null ? 43 : $indexName.hashCode());
		final Object $value = this.getValue();
		result = result * PRIME + ($value == null ? 43 : $value.hashCode());
		return result;
	}

	public String toString() {
		return "GeoIndexedPropertyValue(keyspace=" + this.getKeyspace() + ", indexName=" + this.getIndexName() + ", value="
				+ this.getValue() + ")";
	}
}
