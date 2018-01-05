/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.redis.core.script;

import java.io.IOException;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RedisScript}. Delegates to an underlying {@link ScriptSource} to retrieve script
 * text and detect if script has been modified (and thus should have SHA1 re-calculated). This class is best used as a
 * Singleton to avoid re-calculation of SHA1 on every script execution.
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @param <T> The script result type. Should be one of Long, Boolean, List, or deserialized value type. Can be null if
 *          the script returns a throw-away status (i.e "OK")
 */
public class DefaultRedisScript<T> implements RedisScript<T>, InitializingBean {

	private final Object shaModifiedMonitor = new Object();

	private @Nullable ScriptSource scriptSource;
	private @Nullable String sha1;
	private @Nullable Class<T> resultType;

	/**
	 * Creates a new {@link DefaultRedisScript}
	 */
	public DefaultRedisScript() {
	}

	/**
	 * Creates a new {@link DefaultRedisScript}
	 *
	 * @param script must not be {@literal null}.
	 * @since 2.0
	 */
	public DefaultRedisScript(String script) {
		this(script, null);
	}

	/**
	 * Creates a new {@link DefaultRedisScript}
	 *
	 * @param script must not be {@literal null}.
	 * @param resultType can be {@literal null}.
	 */
	public DefaultRedisScript(String script, @Nullable Class<T> resultType) {

		this.setScriptText(script);
		this.resultType = resultType;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.state(this.scriptSource != null, "Either script, script location," + " or script source is required");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.RedisScript#getSha1()
	 */
	public String getSha1() {

		synchronized (shaModifiedMonitor) {
			if (sha1 == null || scriptSource.isModified()) {
				this.sha1 = DigestUtils.sha1DigestAsHex(getScriptAsString());
			}
			return sha1;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.RedisScript#getResultType()
	 */
	@Nullable
	public Class<T> getResultType() {
		return this.resultType;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.script.RedisScript#getScriptAsString()
	 */
	public String getScriptAsString() {

		try {
			return scriptSource.getScriptAsString();
		} catch (IOException e) {
			throw new ScriptingException("Error reading script text", e);
		}
	}

	/**
	 * @param resultType The script result type. Should be one of Long, Boolean, List, or deserialized value type. Can be
	 *          null if the script returns a throw-away status (i.e "OK")
	 */
	public void setResultType(Class<T> resultType) {
		this.resultType = resultType;
	}

	/**
	 * @param scriptText The script text
	 */
	public void setScriptText(String scriptText) {
		this.scriptSource = new StaticScriptSource(scriptText);
	}

	/**
	 * @param scriptLocation The location of the script
	 */
	public void setLocation(Resource scriptLocation) {
		this.scriptSource = new ResourceScriptSource(scriptLocation);
	}

	/**
	 * @param scriptSource A @{link {@link ScriptSource} pointing to the script
	 */
	public void setScriptSource(ScriptSource scriptSource) {
		this.scriptSource = scriptSource;
	}
}
