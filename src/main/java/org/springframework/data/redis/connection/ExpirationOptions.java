/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Objects;

import org.springframework.lang.Contract;
import org.springframework.util.ObjectUtils;

/**
 * Expiration options for Expiation updates.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.5
 */
public class ExpirationOptions {

	private static final ExpirationOptions NONE = new ExpirationOptions(Condition.ALWAYS);
	private final Condition condition;

	ExpirationOptions(Condition condition) {
		this.condition = condition;
	}

	/**
	 * @return an empty expiration options object.
	 */
	public static ExpirationOptions none() {
		return NONE;
	}

	/**
	 * @return builder for creating {@code FieldExpireOptionsBuilder}.
	 */
	public static ExpirationOptionsBuilder builder() {
		return new ExpirationOptionsBuilder();
	}

	public Condition getCondition() {
		return condition;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ExpirationOptions that = (ExpirationOptions) o;
		return ObjectUtils.nullSafeEquals(this.condition, that.condition);
	}

	@Override
	public int hashCode() {
		return Objects.hash(condition);
	}

	/**
	 * Builder to build {@link ExpirationOptions}
	 */
	public static class ExpirationOptionsBuilder {

		private Condition condition = Condition.ALWAYS;

		private ExpirationOptionsBuilder() {}

		/**
		 * Apply to fields that have no expiration.
		 */
		@Contract("-> this")
		public ExpirationOptionsBuilder nx() {
			this.condition = Condition.NX;
			return this;
		}

		/**
		 * Apply to fields that have an existing expiration.
		 */
		@Contract("-> this")
		public ExpirationOptionsBuilder xx() {
			this.condition = Condition.XX;
			return this;
		}

		/**
		 * Apply to fields when the new expiration is greater than the current one.
		 */
		@Contract("-> this")
		public ExpirationOptionsBuilder gt() {
			this.condition = Condition.GT;
			return this;
		}

		/**
		 * Apply to fields when the new expiration is lower than the current one.
		 */
		@Contract("-> this")
		public ExpirationOptionsBuilder lt() {
			this.condition = Condition.LT;
			return this;
		}

		public ExpirationOptions build() {
			return condition == Condition.ALWAYS ? NONE : new ExpirationOptions(condition);
		}

	}

	/**
	 * Conditions to apply when changing expiration.
	 */
	public enum Condition {

		/**
		 * Always apply expiration.
		 */
		ALWAYS,

		/**
		 * Set expiration only when the field has no expiration.
		 */
		NX,

		/**
		 * Set expiration only when the field has an existing expiration.
		 */
		XX,

		/**
		 * Set expiration only when the new expiration is greater than current one.
		 */
		GT,

		/**
		 * Set expiration only when the new expiration is greater than current one.
		 */
		LT

	}

}
