/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.repository.query;

import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Simple set of operations required to run queries against Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisOperationChain {

	private final Set<PathAndValue> sismember = new LinkedHashSet<>();
	private final Set<PathAndValue> orSismember = new LinkedHashSet<>();

	private @Nullable NearPath near;

	public boolean isEmpty() {
		return near == null && sismember.isEmpty() && orSismember.isEmpty();
	}

	public void sismember(String path, Object value) {
		sismember(new PathAndValue(path, value));
	}

	public void sismember(PathAndValue pathAndValue) {
		sismember.add(pathAndValue);
	}

	public Set<PathAndValue> getSismember() {
		return sismember;
	}

	public void orSismember(String path, Object value) {
		orSismember(new PathAndValue(path, value));
	}

	public void orSismember(PathAndValue pathAndValue) {
		orSismember.add(pathAndValue);
	}

	public void orSismember(Collection<PathAndValue> next) {
		orSismember.addAll(next);
	}

	public Set<PathAndValue> getOrSismember() {
		return orSismember;
	}

	public void near(NearPath near) {

		Assert.notNull(near, "Near must not be null!");
		this.near = near;
	}

	@Nullable
	public NearPath getNear() {
		return near;
	}

	@EqualsAndHashCode
	public static class PathAndValue {

		private final String path;
		private final Collection<Object> values;

		public PathAndValue(String path, Object singleValue) {

			this.path = path;
			this.values = Collections.singleton(singleValue);
		}

		public PathAndValue(String path, @Nullable Collection<Object> values) {

			this.path = path;
			this.values = values != null ? values : Collections.emptySet();
		}

		public boolean isSingleValue() {
			return values.size() == 1;
		}

		public String getPath() {
			return path;
		}

		public Collection<Object> values() {
			return values;
		}

		@Nullable
		public Object getFirstValue() {
			return values.isEmpty() ? null : values.iterator().next();
		}

		@Override
		public String toString() {
			return path + ":" + (isSingleValue() ? getFirstValue() : values);
		}
	}

	/**
	 * @since 1.8
	 * @author Christoph Strobl
	 */
	public static class NearPath extends PathAndValue {

		public NearPath(String path, Point point, Distance distance) {
			super(path, Arrays.asList(point, distance));
		}

		public Point getPoint() {
			return (Point) getFirstValue();
		}

		public Distance getDistance() {

			Iterator<Object> it = values().iterator();
			it.next();
			return (Distance) it.next();
		}
	}
}
