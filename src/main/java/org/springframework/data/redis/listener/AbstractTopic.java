/*
 * Copyright 2023-present the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.listener;

import org.jspecify.annotations.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Abstract base class for defining {@link Topic Topics}.
 *
 * @author John Blum
 * @since 3.1.3
 */
abstract class AbstractTopic implements Topic {

	private final String name;

	AbstractTopic(String name) {
		this.name = name;
	}

	@Override
	public String getTopic() {
		return this.name;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof AbstractTopic that)) {
			return false;
		}

		// Must be exact Topic type
		if (this.getClass() != that.getClass()) {
			return false;
		}

		return ObjectUtils.nullSafeEquals(this.getTopic(), that.getTopic());
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(getTopic());
	}

	@Override
	public String toString() {
		return getTopic();
	}

}
