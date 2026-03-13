/*
 * Copyright 2026-present the original author or authors.
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

import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;

/**
 * @author Christoph Strobl
 */
public abstract class ResetCachesStrategies {

	public interface ResetCachesStrategy {}

	public static ResetCachesStrategy oneByOne() {
		return DefaultResetStrategy.INSTANCE;
	}

	public static ResetCachesStrategy flushing() {
		return FlushingResetStrategy.INSTANCE;
	}

	enum FlushingResetStrategy implements ResetCachesStrategy, CacheWriterOperation<String> {

		INSTANCE;

		@Override
		public String doWithCacheWriter(RedisCacheWriter cacheWriter) {
			return cacheWriter.execute(connection -> {
				connection.serverCommands().flushDb(FlushOption.ASYNC);
				return "ok";
			});
		}
	}

	enum DefaultResetStrategy implements ResetCachesStrategy {
		INSTANCE;
	}
}
