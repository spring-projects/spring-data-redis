/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.annotation;

import java.lang.annotation.*;

/**
 * Annotation that marks a method to be the target of a Redis message listener on the
 * specified {@link #topics() topics} (or {@link #topicPatterns() patterns}).
 *
 * @author Ilyass Bougati
 * @see EnableRedisListeners
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisListener {

    /**
     * The unique identifier of the container managing this endpoint.
     */
    String id() default "";

    /**
     * The bean name of the {@link org.springframework.data.redis.config.RedisListenerContainerFactory}
     * to use to create the message listener container.
     */
    String containerFactory() default "";

    /**
     * The topics for this listener.
     */
    String[] topics() default {};

    /**
     * The topic patterns for this listener.
     */
    String[] topicPatterns() default {};
}
