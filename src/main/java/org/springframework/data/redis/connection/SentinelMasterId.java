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
package org.springframework.data.redis.connection;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Simple {@link NamedNode}.
 *
 * @author Mark Paluch
 * @since 2.5.3
 */
class SentinelMasterId implements NamedNode {

	private final String name;

	public SentinelMasterId(String name) {
		Assert.hasText(name, "Sentinel Master Id must not be null or empty");
		this.name = name;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.NamedNode#getName()
	 */
	@NonNull
	@Override
	public String getName() {
		return name;
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getName();
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof SentinelMasterId)) {
			return false;
		}
		SentinelMasterId that = (SentinelMasterId) o;
		return ObjectUtils.nullSafeEquals(name, that.name);
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(name);
	}
}
