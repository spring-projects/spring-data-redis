package org.springframework.data.redis.test.util;

import org.apache.geode.cache.GemFireCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.gemfire.config.annotation.EnableManager;
import org.springframework.data.gemfire.config.annotation.EnableRedisServer;
import org.springframework.data.gemfire.config.annotation.PeerCacheApplication;

/**
 * The {@link GeodeRedisServer} class bootstraps an Apache Geode peer cache application with the RedisServer
 * and Management Services enabled.
 *
 * The purpose of this test utility class is to run the Spring Data Redis test suite (as a Redis client)
 * against Apache Geode (posing as the Redis server).
 *
 * @author John Blum
 */
@PeerCacheApplication
@EnableManager(start = true)
@EnableRedisServer(bindAddress = "localhost")
public class GeodeRedisServer {

	public static void main(String[] args) {
		AnnotationConfigApplicationContext applicationContext =
			new AnnotationConfigApplicationContext(GeodeRedisServer.class);

		applicationContext.registerShutdownHook();
	}

	@Autowired
	@SuppressWarnings("unused")
	private GemFireCache gemfireCache;

}
