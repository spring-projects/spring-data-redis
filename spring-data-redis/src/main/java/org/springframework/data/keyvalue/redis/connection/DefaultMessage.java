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
package org.springframework.data.keyvalue.redis.connection;


/**
 * Default message implementation.
 * 
 * @author Costin Leau
 */
public class DefaultMessage implements Message {

	private final byte[] payload;
	private final byte[] channel;

	public DefaultMessage(byte[] payload, byte[] channel) {
		this.payload = payload;
		this.channel = channel;
	}

	@Override
	public byte[] getChannel() {
		return channel;
	}

	@Override
	public byte[] getPayload() {
		return payload;
	}
}
