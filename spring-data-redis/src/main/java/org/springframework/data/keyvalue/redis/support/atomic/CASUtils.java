/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.support.atomic;

import java.util.Collections;
import java.util.concurrent.Callable;

import org.springframework.data.keyvalue.redis.core.RedisOperations;
import org.springframework.data.keyvalue.redis.core.SessionCallback;

/**
 * Check-And-Set (CAS) utility. Performs the CAS loop until successful pattern using
 * Redis watch/exec operations. 
 * 
 * The given callback can contain one or multiple reads followed by a multi call
 * and one or multiple writes:
 * 
 * <pre>
 * return CASUtils.execute(ops, key, new Callable<Integer>() {
 *  @Override
 *  public Integer call() throws Exception {
 *    // check
 *    int value = get();
 *    // start MULTI
 *    ops.multi();
 *    // set
 *    ops.increment(key, 1);
 *    return value;
 *  }
 * });
 * </pre>
 * 
 * @author Costin Leau
 */
abstract class CASUtils {

	public static <T, K, V> T execute(final RedisOperations<K, V> ops, final K key, final Callable<T> callback) {
		return ops.execute(new SessionCallback<T>() {
			@Override
			public T execute(RedisOperations operations) {
				try {
					for (;;) {
						operations.watch(Collections.singleton(key));
						T result = callback.call();
						if (operations.exec() != null) {
							return result;
						}
					}
				} catch (Exception ex) {
					// includes DataAccessException
					if (ex instanceof RuntimeException) {
						throw (RuntimeException) ex;
					}
					throw new RuntimeException("Callback threw exception", ex);
				}
			}
		});
	}
}