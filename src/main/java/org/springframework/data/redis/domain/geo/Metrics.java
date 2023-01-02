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

import org.springframework.data.geo.Metric;
import org.springframework.data.redis.connection.RedisGeoCommands;

/**
 * {@link Metric}s supported by Redis.
 *
 * @author Christoph Strobl
 * @since 2.6
 */
public enum Metrics implements Metric {

	METERS(6378137, "m"), KILOMETERS(6378.137, "km"), MILES(3963.191, "mi"), FEET(20925646.325, "ft");

	private final double multiplier;
	private final String abbreviation;

	/**
	 * Creates a new {@link RedisGeoCommands.DistanceUnit} using the given muliplier.
	 *
	 * @param multiplier the earth radius at equator.
	 */
	Metrics(double multiplier, String abbreviation) {

		this.multiplier = multiplier;
		this.abbreviation = abbreviation;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.geo.Metric#getMultiplier()
	 */
	public double getMultiplier() {
		return multiplier;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.geo.Metric#getAbbreviation()
	 */
	@Override
	public String getAbbreviation() {
		return abbreviation;
	}
}
