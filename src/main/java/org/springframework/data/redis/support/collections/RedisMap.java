/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Map view of a Redis hash.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisMap<K, V> extends RedisStore, ConcurrentMap<K, V> {

	Long increment(K key, long delta);

	Double increment(K key, double delta);

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<Map.Entry<K, V>> scan();
}
