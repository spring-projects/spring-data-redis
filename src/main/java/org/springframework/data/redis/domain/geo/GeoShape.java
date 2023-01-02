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

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Shape;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;

/**
 * Search predicate for {@code GEOSEARCH} and {@code GEOSEARCHSTORE} commands.
 *
 * @since 2.6
 */
public interface GeoShape extends Shape {

	/**
	 * Create a shape used as predicate for geo queries from a {@link Distance radius} around the query center point.
	 *
	 * @param radius
	 * @return
	 */
	static GeoShape byRadius(Distance radius) {
		return new RadiusShape(radius);
	}

	/**
	 * Create a shape used as predicate for geo queries from a bounding box with specified by {@code width} and
	 * {@code height}.
	 *
	 * @param width must not be {@literal null}.
	 * @param height must not be {@literal null}.
	 * @param distanceUnit must not be {@literal null}.
	 * @return
	 */
	static GeoShape byBox(double width, double height, DistanceUnit distanceUnit) {
		return byBox(new BoundingBox(width, height, distanceUnit));
	}

	/**
	 * Create a shape used as predicate for geo queries from a {@link BoundingBox}.
	 *
	 * @param boundingBox must not be {@literal null}.
	 * @return
	 */
	static GeoShape byBox(BoundingBox boundingBox) {
		return new BoxShape(boundingBox);
	}

	/**
	 * The metric used for this geo predicate.
	 *
	 * @return
	 */
	Metric getMetric();
}
