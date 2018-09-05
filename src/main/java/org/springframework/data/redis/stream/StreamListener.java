/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.stream;

import org.springframework.data.redis.connection.RedisStreamCommands.StreamMessage;

/**
 * Listener interface to receive delivery of {@link StreamMessage messages}.
 *
 * @author Mark Paluch
 * @param <K> Stream key and Stream field type.
 * @param <V> Stream value type.
 * @since 2.2
 */
@FunctionalInterface
public interface StreamListener<K, V> {

	/**
	 * Callback invoked on receiving a {@link StreamMessage}.
	 *
	 * @param message never {@literal null}.
	 */
	void onMessage(StreamMessage<K, V> message);
}
