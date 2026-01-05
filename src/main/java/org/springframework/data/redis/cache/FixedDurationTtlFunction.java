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
 * limitations under the License.
 */
package org.springframework.data.redis.cache;

import java.time.Duration;

import org.springframework.data.redis.cache.RedisCacheWriter.TtlFunction;
import org.springframework.lang.Nullable;

/**
 * {@link TtlFunction} implementation returning the given, predetermined {@link Duration} used for per cache entry
 * {@literal time-to-live (TTL) expiration}.
 *
 * @author Mark Paluch
 * @author John Blum
 * @see java.time.Duration
 * @see org.springframework.data.redis.cache.RedisCacheWriter.TtlFunction
 * @since 3.2
 */
public record FixedDurationTtlFunction(Duration duration) implements TtlFunction {

	@Override
	public Duration getTimeToLive(Object key, @Nullable Object value) {
		return this.duration;
	}
}
