/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.datastore.redis.core;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.datastore.redis.support.RedisPersistenceExceptionTranslator;

/**
 * Common base class for RedisClientFactories
 * @author Mark Pollack
 *
 */
public abstract class AbstractRedisClientFactory implements RedisClientFactory {

	protected final Log logger = LogFactory.getLog(getClass());
	
	private String hostName;
	
	private int port; 
	
	private String password;	
	
	public int getPort() {
		return port;
	}

	protected void setPort(int port) {
		this.port = port;
	}



	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHostName() {
		return hostName;
	}

	protected void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public RedisClient createClient() {
		return doGetClient();
	}
	
	public abstract RedisClient doGetClient();

	public abstract RedisPersistenceExceptionTranslator getExceptionTranslator();

	
	protected String getDefaultHostName() {
		String temp;
		try {
			InetAddress localMachine = InetAddress.getLocalHost();
			temp = localMachine.getHostName();
			logger.debug("Using hostname [" + temp + "] for hostname.");
		}
		catch (UnknownHostException e) {
			logger.warn("Could not get host name, using 'localhost' as default value", e);
			temp = "localhost";
		}
		return temp;
	}
	
	/*
	public void closeClient() {
		if (logger.isDebugEnabled()) {
			logger.debug("Closing Redis Client: " + this.client);
		}
		try {
			client.close();			
		}
		catch (Throwable ex) {
			logger.debug("Could not close Redis Client", ex);
		}
	}*/

}
