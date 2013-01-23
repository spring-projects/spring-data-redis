/*
 * Copyright 2011-2013 the original author or authors.
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
 * Default message implementation.
 * 
 * @author Costin Leau
 */
public class DefaultMessage implements Message {

	private final byte[] channel;
	private final byte[] body;
	private String toString;

	public DefaultMessage(byte[] channel, byte[] body) {
		this.body = body;
		this.channel = channel;
	}

	
	public byte[] getChannel() {
		return (channel != null ? channel.clone() : null);
	}

	
	public byte[] getBody() {
		return (body != null ? body.clone() : null);
	}

	
	public String toString() {
		if (toString == null){
			toString = new String(body);
		}
		return toString;
	}
}
