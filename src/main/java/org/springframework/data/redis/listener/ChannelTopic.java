/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.listener;

import org.springframework.util.Assert;

/**
 * Channel topic implementation (maps to a Redis channel).
 * 
 * @author Costin Leau
 */
public class ChannelTopic implements Topic {

	private final String channelName;

	/**
	 * Constructs a new <code>ChannelTopic</code> instance.
	 * 
	 * @param name
	 */
	public ChannelTopic(String name) {
		Assert.notNull(name, "a valid topic is required");
		this.channelName = name;
	}

	/**
	 * Returns the topic name.
	 * 
	 * @return topic name
	 */
	public String getTopic() {
		return channelName;
	}

	@Override
	public int hashCode() {
		return channelName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof ChannelTopic)) {
			return false;
		}
		ChannelTopic other = (ChannelTopic) obj;
		if (channelName == null) {
			if (other.channelName != null) {
				return false;
			}
		} else if (!channelName.equals(other.channelName)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return channelName;
	}
}
