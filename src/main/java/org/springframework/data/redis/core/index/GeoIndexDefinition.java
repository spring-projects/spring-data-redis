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
package org.springframework.data.redis.core.index;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 1.8
 */
public class GeoIndexDefinition extends RedisIndexDefinition implements PathBasedRedisIndexDefinition {

	/**
	 * Creates new {@link GeoIndexDefinition}.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param path
	 */
	public GeoIndexDefinition(String keyspace, String path) {
		this(keyspace, path, path);
	}

	/**
	 * Creates new {@link GeoIndexDefinition}.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @param path
	 * @param name must not be {@literal null}.
	 */
	public GeoIndexDefinition(String keyspace, String path, String name) {
		super(keyspace, path, name);
		addCondition(new PathCondition(path));
		setValueTransformer(new PointValueTransformer());
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	static class PointValueTransformer implements IndexValueTransformer {

		@Override
		public Point convert(@Nullable Object source) {

			if (source == null || source instanceof Point) {
				return (Point) source;
			}

			if (source instanceof GeoLocation<?>) {
				return ((GeoLocation<?>) source).getPoint();
			}

			throw new IllegalArgumentException(
					String.format("Cannot convert %s to %s. GeoIndexed property needs to be of type Point or GeoLocation!",
							source.getClass(), Point.class));
		}
	}
}
