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
package org.springframework.data.redis.core;

import java.util.List;

/**
 * Mapper translating Redis bulk value responses (typically returned by a sort query) to actual objects. Implementations
 * of this interface do not have to worry about exception or connection handling.
 * <p>
 * Typically used by {@link RedisTemplate} <tt>sort</tt> methods.
 *
 * @author Costin Leau
 */
public interface BulkMapper<T, V> {

	T mapBulk(List<V> tuple);
}
