/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.redis.repository.configuration;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.redis.core.RedisKeyValueAdapter.EnableKeyspaceEvents;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;

/**
 * Used to enhance configuration options for Redis <a href="https://redis.io/topics/notifications">Keyspace
 * Notifications</a> of {@link EnableRedisRepositories RedisRepositories}.
 *
 * @author Christoph Strobl
 * @since 2.3
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface EnableKeyspaceNotifications {

	/**
	 * Configure usage of {@link KeyExpirationEventMessageListener}.
	 *
	 * @return {@link EnableKeyspaceEvents#ON_DEMAND} by default.
	 */
	EnableKeyspaceEvents enabled() default EnableKeyspaceEvents.ON_DEMAND;

	/**
	 * Configure the database to receive keyspace notifications from. A negative value is used for {@literal all}
	 * databases.
	 *
	 * @return -1 (aka {@literal all databases}) by default.
	 */
	int database() default -1;

	/**
	 * Configure the {@literal notify-keyspace-events} property if not already set. <br />
	 * Use an empty {@link String} to keep (<b>not</b> alter) existing server configuration.
	 *
	 * @return an {@literal empty String} by default.
	 */
	String notifyKeyspaceEvents() default "";
}
