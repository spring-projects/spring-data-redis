/*
 * Copyright 2011-2013 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hash operations bound to a certain key.
 * 
 * @author Costin Leau
 */
public interface BoundHashOperations<H, HK, HV> extends BoundKeyOperations<H> {

	RedisOperations<H, ?> getOperations();

	Boolean hasKey(Object key);

	Long increment(HK key, long delta);

	HV get(Object key);

	void put(HK key, HV value);

	Boolean putIfAbsent(HK key, HV value);

	List<HV> multiGet(Collection<HK> keys);

	void putAll(Map<? extends HK, ? extends HV> m);

	Set<HK> keys();

	List<HV> values();

	Long size();

	void delete(Object key);

	Map<HK, HV> entries();
}
