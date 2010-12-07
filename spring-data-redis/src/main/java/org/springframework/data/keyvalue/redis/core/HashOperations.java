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
package org.springframework.data.keyvalue.redis.core;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Redis map specific operations working on a hash.
 * 
 * @author Costin Leau
 */
public interface HashOperations<H, HK, HV> {

	void delete(H key, Object hashKey);

	Boolean hasKey(H key, Object hashKey);

	HV get(H key, Object hashKey);

	Collection<HV> multiGet(H key, Collection<HK> hashKeys);

	Long increment(H key, HK hashKey, long delta);

	Set<HK> keys(H key);

	Long size(H key);

	void multiSet(H key, Map<? extends HK, ? extends HV> m);

	void set(H key, HK hashKey, HV value);

	Collection<HV> values(H key);

	RedisOperations<H, ?> getOperations();
}
