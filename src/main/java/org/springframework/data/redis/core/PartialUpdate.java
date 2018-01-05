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
package org.springframework.data.redis.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link PartialUpdate} allows to issue individual property updates without the need of rewriting the whole entity. It
 * allows to define {@literal set}, {@literal delete} actions on existing objects while taking care of updating
 * potential expiration times of the entity itself as well as index structures.
 *
 * @author Christoph Strobl
 * @param <T>
 * @since 1.8
 */
public class PartialUpdate<T> {

	private final Object id;
	private final Class<T> target;
	private final @Nullable T value;
	private boolean refreshTtl = false;

	private final List<PropertyUpdate> propertyUpdates = new ArrayList<>();

	private PartialUpdate(Object id, Class<T> target, @Nullable T value, boolean refreshTtl,
			List<PropertyUpdate> propertyUpdates) {

		this.id = id;
		this.target = target;
		this.value = value;
		this.refreshTtl = refreshTtl;
		this.propertyUpdates.addAll(propertyUpdates);
	}

	/**
	 * Create new {@link PartialUpdate} for given id and type.
	 *
	 * @param id must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public PartialUpdate(Object id, Class<T> targetType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(targetType, "TargetType must not be null!");

		this.id = id;
		this.target = (Class<T>) ClassUtils.getUserClass(targetType);
		this.value = null;
	}

	/**
	 * Create new {@link PartialUpdate} for given id and object.
	 *
	 * @param id must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public PartialUpdate(Object id, T value) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(value, "Value must not be null!");

		this.id = id;
		this.target = (Class<T>) ClassUtils.getUserClass(value.getClass());
		this.value = value;
	}

	/**
	 * Create new {@link PartialUpdate} for given id and type.
	 *
	 * @param id must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 */
	public static <S> PartialUpdate<S> newPartialUpdate(Object id, Class<S> targetType) {
		return new PartialUpdate<>(id, targetType);
	}

	/**
	 * @return can be {@literal null}.
	 */
	@Nullable
	public T getValue() {
		return value;
	}

	/**
	 * Set the value of a simple or complex {@literal value} reachable via given {@literal path}.
	 *
	 * @param path must not be {@literal null}.
	 * @param value must not be {@literal null}. If you want to remove a value use {@link #del(String)}.
	 * @return a new {@link PartialUpdate}.
	 */
	public PartialUpdate<T> set(String path, Object value) {

		Assert.hasText(path, "Path to set must not be null or empty!");

		PartialUpdate<T> update = new PartialUpdate<>(this.id, this.target, this.value, this.refreshTtl,
				this.propertyUpdates);
		update.propertyUpdates.add(new PropertyUpdate(UpdateCommand.SET, path, value));

		return update;
	}

	/**
	 * Remove the value reachable via given {@literal path}.
	 *
	 * @param path path must not be {@literal null}.
	 * @return a new {@link PartialUpdate}.
	 */
	public PartialUpdate<T> del(String path) {

		Assert.hasText(path, "Path to remove must not be null or empty!");

		PartialUpdate<T> update = new PartialUpdate<>(this.id, this.target, this.value, this.refreshTtl,
				this.propertyUpdates);
		update.propertyUpdates.add(new PropertyUpdate(UpdateCommand.DEL, path));

		return update;
	}

	/**
	 * Get the target type.
	 *
	 * @return never {@literal null}.
	 */
	public Class<T> getTarget() {
		return target;
	}

	/**
	 * Get the id of the element to update.
	 *
	 * @return never {@literal null}.
	 */
	public Object getId() {
		return id;
	}

	/**
	 * Get the list of individual property updates.
	 *
	 * @return never {@literal null}.
	 */
	public List<PropertyUpdate> getPropertyUpdates() {
		return Collections.unmodifiableList(propertyUpdates);
	}

	/**
	 * @return true if expiration time of target should be updated.
	 */
	public boolean isRefreshTtl() {
		return refreshTtl;
	}

	/**
	 * Set indicator for updating expiration time of target.
	 *
	 * @param refreshTtl
	 * @return a new {@link PartialUpdate}.
	 */
	public PartialUpdate<T> refreshTtl(boolean refreshTtl) {
		return new PartialUpdate<>(this.id, this.target, this.value, refreshTtl, this.propertyUpdates);
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	public static class PropertyUpdate {

		private final UpdateCommand cmd;
		private final String propertyPath;
		private final @Nullable Object value;

		private PropertyUpdate(UpdateCommand cmd, String propertyPath) {
			this(cmd, propertyPath, null);
		}

		private PropertyUpdate(UpdateCommand cmd, String propertyPath, @Nullable Object value) {

			this.cmd = cmd;
			this.propertyPath = propertyPath;
			this.value = value;
		}

		/**
		 * Get the associated {@link UpdateCommand}.
		 *
		 * @return never {@literal null}.
		 */
		public UpdateCommand getCmd() {
			return cmd;
		}

		/**
		 * Get the target path.
		 *
		 * @return never {@literal null}.
		 */
		public String getPropertyPath() {
			return propertyPath;
		}

		/**
		 * Get the value to set.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Object getValue() {
			return value;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	public enum UpdateCommand {
		SET, DEL
	}
}
