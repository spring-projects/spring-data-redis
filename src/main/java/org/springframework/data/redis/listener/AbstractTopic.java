/*
 * Copyright 2017-2023 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Abstract base class for defining {@link Topic Topics}.
 *
 * @author John Blum
 * @see org.springframework.data.redis.listener.Topic
 * @since 3.2.0
 */
abstract class AbstractTopic implements Topic {

	private final String name;

	AbstractTopic(String label, String name) {
		Assert.notNull(name,() -> label + " must not be null");
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
