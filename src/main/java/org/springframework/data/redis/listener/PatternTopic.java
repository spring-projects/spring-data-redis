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
package org.springframework.data.redis.listener;

import org.springframework.util.Assert;

/**
 * Pattern topic (matching multiple channels).
 * 
 * @author Costin Leau
 */
public class PatternTopic implements Topic {

	private final String channelPattern;

	public PatternTopic(String pattern) {
		Assert.notNull(pattern, "a valid topic is required");
		this.channelPattern = pattern;
	}

	public String getTopic() {
		return channelPattern;
	}

	@Override
	public int hashCode() {
		return channelPattern.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof PatternTopic)) {
			return false;
		}
		PatternTopic other = (PatternTopic) obj;
		if (channelPattern == null) {
			if (other.channelPattern != null) {
				return false;
			}
		} else if (!channelPattern.equals(other.channelPattern)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return channelPattern;
	}
}
