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
import org.springframework.util.Assert;

/**
 * Bounding box defined by width and height.
 *
 * @author Mark Paluch
 * @since 2.6
 *
 */
public class BoxShape implements GeoShape {

	private final BoundingBox boundingBox;

	public BoxShape(BoundingBox boundingBox) {

		Assert.notNull(boundingBox, "BoundingBox must not be null");

		this.boundingBox = boundingBox;
	}

	public BoundingBox getBoundingBox() {
		return boundingBox;
	}

	@Override
	public Metric getMetric() {
		return boundingBox.getHeight().getMetric();
	}
}
