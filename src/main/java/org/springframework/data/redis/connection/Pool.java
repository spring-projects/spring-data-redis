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
package org.springframework.data.redis.connection;

/**
 * Pool of resources
 *
 * @author Jennifer Hickey
 */
public interface Pool<T> {

	/**
	 * @return A resource, if available
	 */
	T getResource();

	/**
	 * @param resource A broken resource that should be invalidated
	 */
	void returnBrokenResource(final T resource);

	/**
	 * @param resource A resource to return to the pool
	 */
	void returnResource(final T resource);

	/**
	 * Destroys the pool
	 */
	void destroy();

}
