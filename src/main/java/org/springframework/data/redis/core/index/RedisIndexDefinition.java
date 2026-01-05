/*
 * Copyright 2016-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Base {@link IndexDefinition} implementation.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public abstract class RedisIndexDefinition implements IndexDefinition {

	private final String keyspace;
	private final String indexName;
	private final @Nullable String path;
	private final List<Condition<?>> conditions;

	private @Nullable IndexValueTransformer valueTransformer;

	/**
	 * Creates new {@link RedisIndexDefinition}.
	 *
	 * @param keyspace
	 * @param path
	 * @param indexName
	 */
	protected RedisIndexDefinition(String keyspace, @Nullable String path, String indexName) {

		this.keyspace = keyspace;
		this.indexName = indexName;
		this.path = path;
		this.conditions = new ArrayList<>();
	}

	@Override
	public String getKeyspace() {
		return keyspace;
	}

	@Override
	public Collection<Condition<?>> getConditions() {
		return Collections.unmodifiableCollection(conditions);
	}

	@Override
	public IndexValueTransformer valueTransformer() {
		return valueTransformer != null ? valueTransformer : NoOpValueTransformer.INSTANCE;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Nullable
	public String getPath() {
		return this.path;
	}

	protected void addCondition(Condition<?> condition) {

		Assert.notNull(condition, "Condition must not be null");
		this.conditions.add(condition);
	}

	public void setValueTransformer(IndexValueTransformer valueTransformer) {
		this.valueTransformer = valueTransformer;
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(indexName);
		return result + ObjectUtils.nullSafeHashCode(keyspace);
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RedisIndexDefinition that)) {
			return false;
		}

		if (!ObjectUtils.nullSafeEquals(this.keyspace, that.keyspace)) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(this.indexName, that.indexName);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static enum NoOpValueTransformer implements IndexValueTransformer {
		INSTANCE;

		@Override
		public Object convert(Object source) {
			return source;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static enum LowercaseIndexValueTransformer implements IndexValueTransformer {
		INSTANCE;

		@Override
		public Object convert(Object source) {

			if (!(source instanceof String string)) {
				return source;
			}

			return string.toLowerCase();
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static class CompositeValueTransformer implements IndexValueTransformer {

		private final List<IndexValueTransformer> transformers = new ArrayList<>();

		public CompositeValueTransformer(Collection<IndexValueTransformer> transformers) {
			this.transformers.addAll(transformers);
		}

		@Override
		public Object convert(Object source) {

			Object tmp = source;
			for (IndexValueTransformer transformer : transformers) {
				tmp = transformer.convert(tmp);
			}
			return tmp;
		}

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 * @param <T>
	 */
	public static class OrCondition<T> implements Condition<T> {

		private final List<Condition<T>> conditions = new ArrayList<>();

		public OrCondition(Collection<Condition<T>> conditions) {
			this.conditions.addAll(conditions);
		}

		@Override
		public boolean matches(T value, IndexingContext context) {

			for (Condition<T> condition : conditions) {
				if (condition.matches(value, context)) {
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static class PathCondition implements Condition<Object> {

		private final String path;

		public PathCondition(String path) {
			this.path = path;
		}

		@Override
		public boolean matches(Object value, IndexingContext context) {

			if (!StringUtils.hasText(path)) {
				return true;
			}

			return ObjectUtils.nullSafeEquals(context.getPath(), path);
		}
	}
}
