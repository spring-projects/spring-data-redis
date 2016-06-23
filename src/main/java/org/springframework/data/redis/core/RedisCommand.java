/*
 * Copyright 2014-2016 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Ninad Divadkar
 * @since 1.3
 * @see Redis command list:
 *      https://github.com/antirez/redis/blob/93e7a130fc9594e41ccfc996b5eca7626ae5356a/src/redis.c#L119
 */
public enum RedisCommand {
	// -- A
	APPEND("rw", 2, 2), //
	AUTH("rw", 1, 1), //
	// -- B
	BGREWRITEAOF("r", 0, 0, "bgwriteaof"), //
	BGSAVE("r", 0, 0), //
	BITCOUNT("r", 1, 3), //
	BITOP("rw", 3), //
	BITPOS("r", 2, 4), //
	BLPOP("rw", 2), //
	BRPOP("rw", 2), //
	BRPOPLPUSH("rw", 3), //
	// -- C
	CLIENT_KILL("rw", 1, 1), //
	CLIENT_LIST("r", 0, 0), //
	CLIENT_GETNAME("r", 0, 0), //
	CLIENT_PAUSE("rw", 1, 1), //
	CLIENT_SETNAME("w", 1, 1), //
	CONFIG_GET("r", 1, 1, "getconfig"), //
	CONFIG_REWRITE("rw", 0, 0), //
	CONFIG_SET("w", 2, 2, "setconfig"), //
	CONFIG_RESETSTAT("w", 0, 0, "resetconfigstats"), //
	// -- D
	DBSIZE("r", 0, 0), //
	DECR("w", 1, 1), //
	DECRBY("w", 2, 2), //
	DEL("rw", 1), //
	DISCARD("rw", 0, 0), //
	DUMP("r", 1, 1), //
	// -- E
	ECHO("r", 1, 1), //
	EVAL("rw", 2), //
	EVALSHA("rw", 2), //
	EXEC("rw", 0, 0), //
	EXISTS("r", 1, 1), //
	EXPIRE("rw", 2, 2), //
	EXPIREAT("rw", 2, 2), //
	// -- F
	FLUSHALL("w", 0, 0), //
	FLUSHDB("w", 0, 0), //
	// -- G
	GET("r", 1, 1), //
	GETBIT("r", 2, 2), //
	GETRANGE("r", 3, 3), //
	GETSET("rw", 2, 2), //
	GEOADD("w", 3), //
	GEODIST("r", 2), //
	GEOHASH("r", 2), //
	GEOPOS("r", 2), //
	GEORADIUS("r", 4), //
	GEORADIUSBYMEMBER("r", 3),
	// -- H
	HDEL("rw", 2), //
	HEXISTS("r", 2, 2), //
	HGET("r", 2, 2), //
	HGETALL("r", 1, 1), //
	HINCRBY("rw", 3, 3), //
	HINCBYFLOAT("rw", 3, 3), //
	HKEYS("r", 1), //
	HLEN("r", 1), //
	HMGET("r", 2), //
	HMSET("w", 3), //
	HSET("w", 3, 3), //
	HSETNX("w", 3, 3), //
	HVALS("r", 1, 1), //
	// -- I
	INCR("rw", 1), //
	INCRBYFLOAT("rw", 2, 2), //
	INFO("r", 0), //
	// -- K
	KEYS("r", 1), //
	// -- L
	LASTSAVE("r", 0), //
	LINDEX("r", 2, 2), //
	LINSERT("rw", 4, 4), //
	LLEN("r", 1, 1), //
	LPOP("rw", 1, 1), //
	LPUSH("rw", 2), //
	LPUSHX("rw", 2), //
	LRANGE("r", 3, 3), //
	LREM("rw", 3, 3), //
	LSET("w", 3, 3), //
	LTRIM("w", 3, 3), //
	// -- M
	MGET("r", 1), //
	MIGRATE("rw", 0), //
	MONITOR("rw", 0, 0), //
	MOVE("rw", 2, 2), //
	MSET("w", 2), //
	MSETNX("w", 2), //
	MULTI("rw", 0, 0), //
	// -- P
	PERSIST("rw", 1, 1), //
	PEXPIRE("rw", 2, 2), //
	PEXPIREAT("rw", 2, 2), //
	PING("r", 0, 0), //
	PSETEX("w", 3), //
	PSUBSCRIBE("r", 1), //
	PTTL("r", 1, 1), //
	// -- Q
	QUIT("rw", 0, 0), //
	// -- R
	RANDOMKEY("r", 0, 0), //
	RANAME("w", 2, 2), //
	RENAMENX("w", 2, 2), //
	RESTORE("w", 3, 3), //
	RPOP("rw", 1, 1), //
	RPOPLPUSH("rw", 2, 2), //
	RPUSH("rw", 2), //
	RPUSHX("rw", 2, 2), //
	// -- S
	SADD("rw", 2), //
	SAVE("rw", 0, 0), //
	SCARD("r", 1, 1), //
	SCRIPT_EXISTS("r", 1), //
	SCRIPT_FLUSH("rw", 0, 0), //
	SCRIPT_KILL("rw", 0, 0), //
	SCRIPT_LOAD("rw", 1, 1), //
	SDIFF("r", 1), //
	SDIFFSTORE("rw", 2), //
	SELECT("rw", 0, 0), //
	SET("w", 2), //
	SETBIT("rw", 3, 3), //
	SETEX("w", 3, 3), //
	SETNX("w", 2, 2), //
	SETRANGE("rw", 3, 3), //
	SHUTDOWN("rw", 0), //
	SINTER("r", 1), //
	SINTERSTORE("rw", 2), //
	SISMEMBER("r", 2), //
	SLAVEOF("w", 2), //
	SLOWLOG("rw", 1), //
	SMEMBERS("r", 1, 1), //
	SMOVE("rw", 3, 3), //
	SORT("rw", 1), //
	SPOP("rw", 1, 1), //
	SRANDMEMBER("r", 1, 1), //
	SREM("rw", 2), //
	STRLEN("r", 1, 1), //
	SUBSCRIBE("rw", 1), //
	SUNION("r", 1), //
	SUNIONSTORE("rw ", 2), //
	SYNC("rw", 0, 0), //
	// -- T
	TIME("r", 0, 0), //
	TTL("r", 1, 1), //
	TYPE("r", 1, 1), //
	// -- U
	UNSUBSCRIBE("rw", 0), //
	UNWATCH("rw", 0, 0), //
	// -- W
	WATCH("rw", 1), //
	// -- Z
	ZADD("rw", 3), //
	ZCARD("r", 1), //
	ZCOUNT("r", 3, 3), //
	ZINCRBY("rw", 3), //
	ZINTERSTORE("rw", 3), //
	ZRANGE("r", 3), //
	ZRANGEBYSCORE("r", 3), //
	ZRANK("r", 2, 2), //
	ZREM("rw", 2), //
	ZREMRANGEBYRANK("rw", 3, 3), //
	ZREMRANGEBYSCORE("rm", 3, 3), //
	ZREVRANGE("r", 3), //
	ZREVRANGEBYSCORE("r", 3), //
	ZREVRANK("r", 2, 2), //
	ZSCORE("r", 2, 2), //
	ZUNIONSTORE("rw", 3), //
	SCAN("r", 1), //
	SSCAN("r", 2), //
	HSCAN("r", 2), //
	ZSCAN("r", 2), //
	// -- UNKNOWN / DEFAULT
	UNKNOWN("rw", -1);

	private boolean read = true;
	private boolean write = true;
	private Set<String> alias = new HashSet<String>(1);

	private int minArgs = -1;
	private int maxArgs = -1;

	private final static Map<String, RedisCommand> commandLookup;

	static {
		commandLookup = buildCommandLookupTable();
	}

	private static Map<String, RedisCommand> buildCommandLookupTable() {

		RedisCommand[] cmds = RedisCommand.values();
		Map<String, RedisCommand> map = new HashMap<String, RedisCommand>(cmds.length, 1.0F);

		for (RedisCommand cmd : cmds) {

			map.put(cmd.name().toLowerCase(), cmd);

			for (String alias : cmd.alias) {
				map.put(alias, cmd);
			}
		}

		return Collections.unmodifiableMap(map);
	}

	private RedisCommand(String mode, int minArgs) {
		this(mode, minArgs, -1);
	}

	private RedisCommand(String mode, int minArgs, int maxArgs) {

		if (StringUtils.hasText(mode)) {
			this.read = mode.toLowerCase().indexOf('r') > -1;
			this.write = mode.toLowerCase().indexOf('w') > -1;
		}

		this.minArgs = minArgs;
		this.maxArgs = maxArgs;
	}

	/**
	 * Creates a new {@link RedisCommand}.
	 * 
	 * @param mode
	 * @param minArgs
	 * @param maxArgs
	 * @param alias
	 */
	private RedisCommand(String mode, int minArgs, int maxArgs, String... alias) {

		this(mode, minArgs, maxArgs);

		if (alias != null && alias.length > 0) {
			this.alias.addAll(Arrays.asList(alias));
		}
	}

	/**
	 * @return {@literal true} if the command requires arguments
	 */
	public boolean requiresArguments() {
		return minArgs >= 0;
	}

	/**
	 * @return {@literal true} if an exact number of arguments is expected
	 */
	public boolean requiresExactNumberOfArguments() {
		return maxArgs >= 0;
	}

	/**
	 * @return {@literal true} if the command triggers a read operation
	 */
	public boolean isRead() {
		return read;
	}

	/**
	 * @return {@literal true} if the command triggers a write operation
	 */
	public boolean isWrite() {
		return write;
	}

	/**
	 * @return {@literal true} if values are read but not written
	 */
	public boolean isReadonly() {
		return read && !write;
	}

	/**
	 * {@link String#equalsIgnoreCase(String)} compare the given string representation of {@literal command} against the
	 * {@link #toString()} representation of the command as well as its given {@link #alias}.
	 * 
	 * @param command
	 * @return true if positive match.
	 */
	public boolean isRepresentedBy(String command) {

		if (!StringUtils.hasText(command)) {
			return false;
		}

		if (toString().equalsIgnoreCase(command)) {
			return true;
		}

		return alias.contains(command.toLowerCase());
	}

	/**
	 * Validates given argument count against expected ones.
	 * 
	 * @param nrArguments
	 * @exception IllegalArgumentException in case argument count does not match expected.
	 */
	public void validateArgumentCount(int nrArguments) {

		if (requiresArguments()) {
			if (requiresExactNumberOfArguments()) {
				if (nrArguments != maxArgs) {
					throw new IllegalArgumentException(
							String.format("%s command requires %s arguments.", this.name(), this.maxArgs));
				}
			}
			if (nrArguments < minArgs) {
				throw new IllegalArgumentException(
						String.format("%s command requires at least %s arguments.", this.name(), this.minArgs));
			}
		}
	}

	/**
	 * Returns the command represented by the given {@code key}. Returns {@link #UNKNOWN} if no matching command could be
	 * found.
	 * 
	 * @param key
	 * @return
	 */
	public static RedisCommand failsafeCommandLookup(String key) {

		if (!StringUtils.hasText(key)) {
			return RedisCommand.UNKNOWN;
		}

		RedisCommand cmd = commandLookup.get(key.toLowerCase());
		if (cmd != null) {
			return cmd;
		}

		return RedisCommand.UNKNOWN;
	}
}
