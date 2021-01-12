/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.test.condition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * {@code @EnabledOnRedisDriver} is used to signal that the annotated test class or test method is only <em>enabled</em>
 * when one of the {@link #value() specified Redis Clients} is used.
 * <p>
 * This annotation must be used in combination with {@link DriverQualifier @DriverQualifier} so the extension can
 * identify the driver from a {@link org.springframework.data.redis.connection.RedisConnectionFactory}.
 * <p>
 * If a test method is disabled via this annotation, that does not prevent the test class from being instantiated.
 * Rather, it prevents the execution of the test method and method-level lifecycle callbacks such as {@code @BeforeEach}
 * methods, {@code @AfterEach} methods, and corresponding extension APIs.
 *
 * @author Thomas Darimont
 * @author Mark Paluch
 * @see org.springframework.data.redis.connection.RedisConnectionFactory
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(EnabledOnRedisDriverCondition.class)
public @interface EnabledOnRedisDriver {

	/**
	 * One or more Redis drivers that enable the condition.
	 */
	RedisDriver[] value() default {};

	/**
	 * Annotation to identify the field that holds the
	 * {@link org.springframework.data.redis.connection.RedisConnectionFactory} to inspect.
	 */
	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@interface DriverQualifier {

	}

}
