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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Represents a geospatial bounding box defined by width and height.
 *
 * @author Mark Paluch
 * @since 2.6
 */
public class BoundingBox implements Shape {

	private static final long serialVersionUID = 5215611530535947924L;

	private final Distance width;
	private final Distance height;

	/**
	 * Creates a new {@link BoundingBox} from the given width and height. Both distances must use the same
	 * {@link Metric}.
	 *
	 * @param width must not be {@literal null}.
	 * @param height must not be {@literal null}.
	 */
	public BoundingBox(Distance width, Distance height) {

		Assert.notNull(width, "Width must not be null!");
		Assert.notNull(height, "Height must not be null!");
		Assert.isTrue(width.getMetric().equals(height.getMetric()), "Metric for width and height must be the same!");

		this.width = width;
		this.height = height;
	}

	/**
	 * Creates a new {@link BoundingBox} from the given width, height and {@link Metric}.
	 *
	 * @param width
	 * @param height
	 * @param metric must not be {@literal null}.
	 */
	public BoundingBox(double width, double height, Metric metric) {
		this(new Distance(width, metric), new Distance(height, metric));
	}

	/**
	 * Returns the width of this bounding box.
	 *
	 * @return will never be {@literal null}.
	 */
	public Distance getWidth() {
		return width;
	}

	/**
	 * Returns the height of this bounding box.
	 *
	 * @return will never be {@literal null}.
	 */
	public Distance getHeight() {
		return height;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(width);
		result = 31 * result + ObjectUtils.nullSafeHashCode(height);
		return result;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof BoundingBox)) {
			return false;
		}
		BoundingBox that = (BoundingBox) o;
		if (!ObjectUtils.nullSafeEquals(width, that.width)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(height, that.height);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Bounding box: [width=%s, height=%s]", width, height);
	}
}
