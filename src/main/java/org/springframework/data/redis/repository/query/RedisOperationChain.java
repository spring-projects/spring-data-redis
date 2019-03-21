/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.repository.query;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.util.ObjectUtils;

/**
 * Simple set of operations requried to run queries against Redis.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisOperationChain {

	private Set<PathAndValue> sismember = new LinkedHashSet<PathAndValue>();
	private Set<PathAndValue> orSismember = new LinkedHashSet<PathAndValue>();

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

	public static class PathAndValue {

		private final String path;
		private final Collection<Object> values;

		public PathAndValue(String path, Object singleValue) {

			this.path = path;
			this.values = Collections.singleton(singleValue);
		}

		public PathAndValue(String path, Collection<Object> values) {

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

		public Object getFirstValue() {
			return values.isEmpty() ? null : values.iterator().next();
		}

		@Override
		public String toString() {
			return path + ":" + (isSingleValue() ? getFirstValue() : values);
		}

		@Override
		public int hashCode() {

			int result = ObjectUtils.nullSafeHashCode(path);
			result += ObjectUtils.nullSafeHashCode(values);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof PathAndValue)) {
				return false;
			}
			PathAndValue that = (PathAndValue) obj;
			if (!ObjectUtils.nullSafeEquals(this.path, that.path)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.values, that.values);
		}

	}

}
