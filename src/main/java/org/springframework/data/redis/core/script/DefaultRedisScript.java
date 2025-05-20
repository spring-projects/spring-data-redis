/*
 * Copyright 2013-2025 the original author or authors.
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
package org.springframework.data.redis.core.script;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
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

	private @Nullable Class<T> resultType;

	private final Lock lock = new ReentrantLock();

	private @Nullable ScriptSource scriptSource;

	private @Nullable String sha1;

	/**
	 * Creates a new {@link DefaultRedisScript}
	 */
	public DefaultRedisScript() {}

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

	@Override
	public void afterPropertiesSet() {
		Assert.state(this.scriptSource != null, "Either script, script location," + " or script source is required");
	}

	@Override
	public String getSha1() {

		lock.lock();

		try {
			if (sha1 == null || scriptSource.isModified()) {
				this.sha1 = DigestUtils.sha1DigestAsHex(getScriptAsString());
			}
			return sha1;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public @Nullable Class<T> getResultType() {
		return this.resultType;
	}

	@Override
	public String getScriptAsString() {

		try {
			return scriptSource.getScriptAsString();
		} catch (IOException ex) {
			throw new ScriptingException("Error reading script text", ex);
		}
	}

	/**
	 * @param resultType The script result type. Should be one of Long, Boolean, List, or deserialized value type. Can be
	 *          {@literal null} if the script returns a throw-away status (i.e "OK")
	 */
	public void setResultType(@Nullable Class<T> resultType) {
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
