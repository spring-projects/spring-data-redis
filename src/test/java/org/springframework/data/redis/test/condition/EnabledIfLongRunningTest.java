/*
 * Copyright 2018-2020 the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * {@code @EnabledIfLongRunningTest} is used to signal that the annotated test method or test class is only
 * <em>enabled</em> if long running tests are enabled. This is a meta-annotation for
 * {@code @EnabledIfSystemProperty(named = "longRunningTest")}.
 * <p>
 * When declared at the class level, the result will apply to all test methods within that class as well.
 *
 * @author Mark Paluch
 */
@Target({ ElementType.ANNOTATION_TYPE, ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@EnabledIfSystemProperty(named = "runLongTests", matches = "true", disabledReason = "Long-running tests disabled")
public @interface EnabledIfLongRunningTest {

}
