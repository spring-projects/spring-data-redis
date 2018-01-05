/*
 * Copyright 2015-2018 the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import org.springframework.data.annotation.ReadOnlyProperty;

/**
 * {@link TimeToLive} marks a single numeric property on aggregate root to be used for setting expirations in Redis. The
 * annotated property supersedes any other timeout configuration.
 *
 * <pre>
 * <code>
 * &#64;RedisHash
 * class Person {
 *   &#64;Id String id;
 *   String name;
 *   &#64;TimeToLive Long timeout;
 * }
 * </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @since 1.7
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(value = { ElementType.FIELD, ElementType.METHOD })
@ReadOnlyProperty
public @interface TimeToLive {

	/**
	 * {@link TimeUnit} unit to use.
	 *
	 * @return {@link TimeUnit#SECONDS} by default.
	 */
	TimeUnit unit() default TimeUnit.SECONDS;
}
