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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default message implementation.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public class DefaultMessage implements Message {

	private final byte[] channel;
	private final byte[] body;
	private @Nullable String toString;

	public DefaultMessage(byte[] channel, byte[] body) {

		Assert.notNull(channel, "Channel must not be null!");
		Assert.notNull(body, "Body must not be null!");

		this.body = body;
		this.channel = channel;
	}

	/**
	 * @return
	 */
	public byte[] getChannel() {
		return channel.clone();
	}

	public byte[] getBody() {
		return body.clone();
	}

	@Override
	public String toString() {

		if (toString == null) {
			toString = new String(body);
		}
		return toString;
	}
}
