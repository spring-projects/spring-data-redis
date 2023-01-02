/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.redis.domain.geo;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 2.6
 */
public class GeoLocation<T> {

	private final T name;
	private final Point point;

	public GeoLocation(T name, Point point) {
		this.name = name;
		this.point = point;
	}

	public T getName() {
		return this.name;
	}

	public Point getPoint() {
		return this.point;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof GeoLocation)) {
			return false;
		}

		GeoLocation<?> that = (GeoLocation<?>) o;

		if (!ObjectUtils.nullSafeEquals(name, that.name)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(point, that.point);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(name);
		result = 31 * result + ObjectUtils.nullSafeHashCode(point);
		return result;
	}

	public String toString() {
		return "GeoLocation(name=" + this.getName() + ", point=" + this.getPoint() + ")";
	}
}
