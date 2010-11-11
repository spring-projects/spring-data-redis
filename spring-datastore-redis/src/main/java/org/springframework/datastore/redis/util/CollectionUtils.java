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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.datastore.redis.serializer.RedisSerializer;

/**
 * Utility class used mainly for type conversion by the default collection implementations.
 * 
 * @author Costin Leau
 */
abstract class CollectionUtils {

	static <E> List<E> deserializeAsList(List<String> input, RedisSerializer serializer) {
		List<E> result = new ArrayList<E>(input.size());
		for (String string : input) {
			E item = serializer.deserialize(string);
			result.add(item);
		}
		return result;
	}

	static <E> Collection<E> reverse(Collection<? extends E> c) {
		List<E> reverse = new ArrayList<E>(c.size());

		int index = c.size();
		for (E e : c) {
			reverse.add(--index, e);
		}

		return reverse;
	}
}
