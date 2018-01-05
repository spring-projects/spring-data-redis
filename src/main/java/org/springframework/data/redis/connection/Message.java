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

import java.io.Serializable;

import org.springframework.lang.Nullable;

/**
 * Class encapsulating a Redis message body and its properties.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface Message extends Serializable {

	/**
	 * Returns the body (or the payload) of the message.
	 *
	 * @return message body. Never {@literal null}.
	 */
	byte[] getBody();

	/**
	 * Returns the channel associated with the message.
	 *
	 * @return message channel. Never {@literal null}.
	 */
	byte[] getChannel();
}
