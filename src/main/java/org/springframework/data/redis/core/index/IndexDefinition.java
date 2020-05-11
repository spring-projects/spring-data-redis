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
package org.springframework.data.redis.core.index;

import java.util.Collection;

import org.springframework.data.util.TypeInformation;
import org.springframework.util.ObjectUtils;

/**
 * {@link IndexDefinition} allow to set up a blueprint for creating secondary index structures in Redis. Setting up
 * conditions allows to define {@link Condition} that have to be passed in order to add a value to the index. This
 * allows to fine grained tune the index structure. {@link IndexValueTransformer} gets applied to the raw value for
 * creating the actual index entry.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public interface IndexDefinition {

	/**
	 * @return never {@literal null}.
	 */
	String getKeyspace();

	/**
	 * @return never {@literal null}.
	 */
	Collection<Condition<?>> getConditions();

	/**
	 * @return never {@literal null}.
	 */
	IndexValueTransformer valueTransformer();

	/**
	 * @return never {@literal null}.
	 */
	String getIndexName();

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 * @param <T>
	 */
	interface Condition<T> {
		boolean matches(T value, IndexingContext context);
	}

	/**
	 * Context in which a particular value is about to get indexed.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	final class IndexingContext {

		private final String keyspace;
		private final String path;
		private final TypeInformation<?> typeInformation;

		public IndexingContext(String keyspace, String path, TypeInformation<?> typeInformation) {

			this.keyspace = keyspace;
			this.path = path;
			this.typeInformation = typeInformation;
		}

		public String getKeyspace() {
			return this.keyspace;
		}

		public String getPath() {
			return this.path;
		}

		public TypeInformation<?> getTypeInformation() {
			return this.typeInformation;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			IndexingContext that = (IndexingContext) o;

			if (!ObjectUtils.nullSafeEquals(keyspace, that.keyspace)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(path, that.path)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(typeInformation, that.typeInformation);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(keyspace);
			result = 31 * result + ObjectUtils.nullSafeHashCode(path);
			result = 31 * result + ObjectUtils.nullSafeHashCode(typeInformation);
			return result;
		}

		public String toString() {
			return "IndexDefinition.IndexingContext(keyspace=" + this.getKeyspace() + ", path=" + this.getPath()
					+ ", typeInformation=" + this.getTypeInformation() + ")";
		}
	}
}
