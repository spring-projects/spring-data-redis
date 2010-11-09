/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.util;

import java.util.Set;

/**
 * Redis extension for the {@link Set} contract. Supports {@link Set} specific
 * operations backed by Redis commands.
 * 
 * @author Costin Leau
 */
public interface RedisSet extends RedisStore, Set<String> {

	Set<String> intersect(RedisSet... sets);

	Set<String> union(RedisSet... sets);

	Set<String> diff(RedisSet... sets);

	RedisSet intersectAndStore(String destKey, RedisSet... sets);

	RedisSet unionAndStore(String destKey, RedisSet... sets);

	RedisSet diffAndStore(String destKey, RedisSet... sets);
}
