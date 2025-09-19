/*
 * Copyright 2014-2025 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link Enum Enumeration} of well-known {@literal Redis commands}. This enumeration serves as non-exhaustive set of
 * built-in commands for a typical Redis server.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Ninad Divadkar
 * @author Mark Paluch
 * @author Oscar Cai
 * @author Sébastien Volle
 * @author John Blum
 * @author LeeHyungGeol
 * @since 1.3
 * @link <a href=
 *       "https://github.com/antirez/redis/blob/843de8b786562d8d77c78d83a971060adc61f77a/src/server.c#L180">Redis
 *       command list</a>
 */
public enum RedisCommand {

	// -- A
	APPEND("rw", 2, 2), //
	AUTH("rw", 1, 1), //

	// -- B
	BGREWRITEAOF("r", 0, 0, "bgwriteaof"), //
	BGSAVE("r", 0, 0), //
	BITCOUNT("r", 1, 3), //
	BITFIELD("rw", 1), //
	BITFIELD_RO("r", 1),
	BITOP("rw", 3), //
	BITPOS("r", 2, 4), //
	BLMOVE("rw", 4), //
	BLMPOP("rw", 4), //
	BLPOP("rw", 2), //
	BRPOP("rw", 2), //
	BRPOPLPUSH("rw", 3), //
	BZMPOP("rw", 3), //
	BZPOPMAX("rw", 2), //
	BZPOPMIN("rw", 2), //

	// -- C
	CLIENT_GETREDIR("r", 0, 0), //
	CLIENT_ID("r", 0, 0), //
	CLIENT_INFO("r", 0, 0), //
	CLIENT_KILL("rw", 1, 1), //
	CLIENT_LIST("r", 0, 0), //
	CLIENT_GETNAME("r", 0, 0), //
	CLIENT_PAUSE("rw", 1, 1), //
	CLIENT_SETINFO("w", 1), //
	CLIENT_SETNAME("w", 1, 1), //
	CLIENT_NO_EVICT("w", 1, 1, "client no-evict"), //
	CLIENT_NO_TOUCH("w", 1, 1,  "client no-touch"), //
	CLIENT_TRACKING("rw", 1), //
	CONFIG_GET("r", 1, 1, "getconfig"), //
	CONFIG_REWRITE("rw", 0, 0), //
	CONFIG_SET("w", 2, 2, "setconfig"), //
	CONFIG_RESETSTAT("w", 0, 0, "resetconfigstats"), //
	COPY("rw", 2), //

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
	EVAL_RO("r", 2), //
	EVALSHA("rw", 2), //
	EVALSHA_RO("r", 2), //
	EXEC("rw", 0, 0), //
	EXISTS("r", 1, 1), //
	EXPIRE("rw", 2), //
	EXPIREAT("rw", 2), //
	EXPIRETIME("r", 1), //

	// -- F
	FCALL("rw", 2), //
	FCALL_RO("r", 2), //
	FLUSHALL("w", 0, 0), //
	FLUSHDB("w", 0, 0), //
	FUNCTION_DELETE("w", 1), //
	FUNCTION_DUMP("w", 0, 0), //
	FUNCTION_FLUSH("w", 0, 0), //
	FUNCTION_KILL("w", 0, 0), //

	// -- G
	GET("r", 1, 1), //
	GETBIT("r", 2, 2), //
	GETDEL("rw", 1), //
	GETEX("rw", 1), //
	GETRANGE("r", 3, 3), //
	GETSET("rw", 2, 2), //
	GEOADD("w", 3), //
	GEODIST("r", 2), //
	GEOHASH("r", 2), //
	GEOPOS("r", 2), //
	GEORADIUS("rw", 4), //
	GEORADIUS_RO("r", 4), //
	GEORADIUSBYMEMBER("rw", 3), //
	GEORADIUSBYMEMBER_RO("r", 3), //
	GEOSEARCH("r", 1), //
	GEOSEARCH_STORE("rw", 1), //

	// -- H
	HDEL("rw", 2), //
	HELLO("rw", 0, 0), //
	HEXISTS("r", 2, 2), //
	HGET("r", 2, 2), //
	HGETALL("r", 1, 1), //
	HGETDEL("rw", 2), //
	HGETEX("rw", 2), //
	HINCRBY("rw", 3, 3), //
	HINCBYFLOAT("rw", 3, 3), //
	HKEYS("r", 1), //
	HLEN("r", 1), //
	HMGET("r", 2), //
	HMSET("w", 3), //
	HPOP("rw", 3),
	HSET("w", 3, 3), //
    HSETEX("w", 3), //
	HSETNX("w", 3, 3), //
	HVALS("r", 1, 1), //
	HEXPIRE("w", 5), //
	HEXPIREAT("w", 5), //
	HPEXPIRE("w", 5), //
	HPEXPIREAT("w", 5), //
	HPEXPIRETIME("r", 4), //
	HPERSIST("w", 4), //
	HTTL("r", 4), //
	HPTTL("r", 4), //
	HSCAN("r", 2), //
	HSTRLEN("r", 2), //

	// -- I
	INCR("rw", 1), //
	INCRBY("rw", 2, 2), //
	INCRBYFLOAT("rw", 2, 2), //
	INFO("r", 0), //

	// -- K
	KEYS("r", 1), //

	// -- L
	LCS("r", 2), //
	LASTSAVE("r", 0), //
	LINDEX("r", 2, 2), //
	LINSERT("rw", 4, 4), //
	LLEN("r", 1, 1), //
	LMOVE("rw", 2), //
	LMPOP("rw", 2), //
	LPOP("rw", 1, 1), //
	LPOS("r", 2), //
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
	PEXPIRE("rw", 2), //
	PEXPIREAT("rw", 2), //
	PEXPIRETIME("r", 1), //
	PFADD("w", 10), //
	PFCOUNT("r", 1), //
	PFMERGE("rw", 2), //
	PING("r", 0, 0), //
	PSETEX("w", 3), //
	PSUBSCRIBE("r", 1), //
	PTTL("r", 1, 1), //
	// -- Q
	QUIT("rw", 0, 0), //

	// -- R
	RANDOMKEY("r", 0, 0), //
	READONLY("w", 0, 0), //
	READWRITE("w", 0, 0), //
	RENAME("w", 2, 2), //
	RENAMENX("w", 2, 2), //
	REPLICAOF("w", 2), //
	RESTORE("w", 3, 3), //
	RPOP("rw", 1, 1), //
	RPOPLPUSH("rw", 2, 2), //
	RPUSH("rw", 2), //
	RPUSHX("rw", 2, 2), //

	// -- S
	SADD("rw", 2), //
	SAVE("rw", 0, 0), //
	SCAN("r", 1), //
	SCARD("r", 1, 1), //
	SCRIPT_EXISTS("r", 1), //
	SCRIPT_FLUSH("rw", 0, 0), //
	SCRIPT_KILL("rw", 0, 0), //
	SCRIPT_LOAD("rw", 1, 1), //
	SDIFF("r", 1), //
	SDIFFSTORE("rw", 2), //
	SELECT("rw", 1, 1), //
	SET("w", 2), //
	SETBIT("rw", 3, 3), //
	SETEX("w", 3, 3), //
	SETNX("w", 2, 2), //
	SETRANGE("rw", 3, 3), //
	SHUTDOWN("rw", 0), //
	SINTER("r", 1), //
	SINTERCARD("r", 1), //
	SINTERSTORE("rw", 2), //
	SISMEMBER("r", 2), //
	SLAVEOF("w", 2), //
	SLOWLOG("rw", 1), //
	SMEMBERS("r", 1, 1), //
	SMOVE("rw", 3, 3), //
	SORT("rw", 1), //
	SORT_RO("r", 1), //
	SPOP("rw", 1, 1), //
	SRANDMEMBER("r", 1, 1), //
	SREM("rw", 2), //
	SSCAN("r", 1), //
	STRLEN("r", 1, 1), //
	SUBSCRIBE("rw", 1), //
	SUBSTR("r", 3), //
	SUNION("r", 1), //
	SUNIONSTORE("rw ", 2), //
	SYNC("rw", 0, 0), //

	// -- T
	TIME("r", 0, 0), //
	TTL("r", 1, 1), //
	TYPE("r", 1, 1), //

	// -- U
	UNLINK("w", 1), //
	UNSUBSCRIBE("rw", 0), //
	UNWATCH("rw", 0, 0), //

	// -- V
	VADD("w", 3), //
	VCARD("r", 1), //
	VDIM("r", 1), //
	VEMB("r", 2), //
	VISMEMBER("r", 2), //
	VLINKS("r", 2, 3), //
	VRANDMEMBER("r", 1, 2), //
	VREM("w", 2), //
	VSIM("w", 1), //

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
	ZRANGEWITHSCORES("r", 3), //
	ZRANGEBYSCOREWITHSCORES("r", 2), //
	ZRANK("r", 2, 2), //
	ZREM("rw", 2), //
	ZREMRANGEBYRANK("rw", 3, 3), //
	ZREMRANGEBYSCORE("rw", 3, 3), //
	ZREVRANGE("r", 3), //
	ZREVRANGEBYSCORE("r", 3), //
	ZREVRANGEWITHSCORES("r", 3), //
	ZREVRANGEBYSCOREWITHSCORES("r", 2), //
	ZREVRANK("r", 2, 2), //
	ZSCORE("r", 2, 2), //
	ZUNIONSTORE("rw", 3), //
	ZSCAN("r", 2), //

	// -- UNKNOWN / DEFAULT
	UNKNOWN("rw", -1);

	private static final Map<String, RedisCommand> commandLookup;

	static {
		commandLookup = buildCommandLookupTable();
	}

	private static Map<String, RedisCommand> buildCommandLookupTable() {

		RedisCommand[] commands = RedisCommand.values();
		Map<String, RedisCommand> map = new HashMap<>(commands.length + 5, 1.0f);

		for (RedisCommand command : commands) {

			map.put(command.name().toLowerCase(), command);

			if (!ObjectUtils.isEmpty(command.alias)) {
				map.put(command.alias, command);
			}
		}

		return Collections.unmodifiableMap(map);
	}

	/**
	 * Returns the command represented by the given {@code key}, otherwise returns {@link #UNKNOWN} if no matching command
	 * could be found.
	 *
	 * @param key {@link String key} to the {@link RedisCommand} to lookup.
	 * @return a matching {@link RedisCommand} for the given {@code key}, otherwise {@link #UNKNOWN}.
	 */
	public static RedisCommand failsafeCommandLookup(String key) {
		return StringUtils.hasText(key) ? commandLookup.getOrDefault(key.toLowerCase(), UNKNOWN) : UNKNOWN;
	}

	private final boolean read;
	private final boolean write;

	private final int minArgs;
	private final int maxArgs;

	private final @Nullable String alias;

	/**
	 * Creates a new {@link RedisCommand}.
	 *
	 * @param mode {@link String} containing the mode ({@literal r} for read, {@literal w} for write or {@literal rw} for
	 *          read-write) of the Redis command.
	 * @param minArgs minimum number of arguments accepted by the Redis command.
	 */
	RedisCommand(String mode, int minArgs) {
		this(mode, minArgs, -1);
	}

	/**
	 * Creates a new {@link RedisCommand}.
	 *
	 * @param mode {@link String} containing the mode ({@literal r} for read, {@literal w} for write or {@literal rw} for
	 *          read-write) of the Redis command.
	 * @param minArgs minimum number of arguments accepted by the Redis command.
	 * @param maxArgs maximum number of arguments accepted by the Redis command.
	 */
	RedisCommand(String mode, int minArgs, int maxArgs) {
		this(mode, minArgs, maxArgs, null);
	}

	/**
	 * Creates a new {@link RedisCommand}.
	 *
	 * @param mode {@link String} containing the mode ({@literal r} for read, {@literal w} for write or {@literal rw} for
	 *          read-write) of the Redis command.
	 * @param minArgs minimum number of arguments accepted by the Redis command.
	 * @param maxArgs maximum number of arguments accepted by the Redis command.
	 * @param alias alternate command name used as alias for the Redis command, can be {@literal null}.
	 */
	RedisCommand(String mode, int minArgs, int maxArgs, @Nullable String alias) {

		if (StringUtils.hasText(mode)) {
			this.read = mode.toLowerCase().contains("r");
			this.write = mode.toLowerCase().contains("w");
		} else {
			this.read = true;
			this.write = true;
		}

		this.minArgs = minArgs;
		this.maxArgs = maxArgs;
		this.alias = alias;
	}

	/**
	 * @return {@literal true} if the command requires arguments
	 */
	public boolean requiresArguments() {
		return minArgs > 0;
	}

	/**
	 * @return {@literal true} if an exact number of arguments is expected.
	 */
	public boolean requiresExactNumberOfArguments() {
		return maxArgs == 0 || minArgs == maxArgs;
	}

	/**
	 * Returns a {@link Set} of all {@link String aliases} for this {@link RedisCommand}.
	 *
	 * @return a {@link Set} of all {@link String aliases} for this {@link RedisCommand}.
	 */
	Set<String> getAliases() {
		return ObjectUtils.isEmpty(this.alias) ? Collections.emptySet() : Collections.singleton(this.alias);
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
		return isRead() && !isWrite();
	}

	/**
	 * {@link String#equalsIgnoreCase(String) Compares} the given {@link String} representing the {@literal Redis command}
	 * to the {@link #toString()} representation of {@link RedisCommand} as well as any {@link #alias}.
	 *
	 * @param command {@link String} representation of the {@literal Redis command} to match.
	 * @return {@literal true} if a positive match.
	 */
	public boolean isRepresentedBy(String command) {

		return StringUtils.hasText(command)
				&& (toString().equalsIgnoreCase(command) || command.toLowerCase().equals(this.alias));
	}

	/**
	 * Validates given {@link Integer argument count} against expected ones.
	 *
	 * @param argumentCount {@link Integer number of arguments} passed to the Redis command.
	 * @exception IllegalArgumentException if the given {@link Integer argument count} does not match expected.
	 */
	public void validateArgumentCount(int argumentCount) {

		if (requiresArguments()) {
			if (requiresExactNumberOfArguments()) {
				if (argumentCount != this.maxArgs) {
					throw new IllegalArgumentException(
							"%s command requires %d %s".formatted(name(), this.maxArgs, arguments(this.maxArgs)));
				}
			}
			if (argumentCount < this.minArgs) {
				throw new IllegalArgumentException(
						"%s command requires at least %d %s".formatted(name(), this.minArgs, arguments(this.maxArgs)));
			}
			if (this.maxArgs > 0 && argumentCount > this.maxArgs) {
				throw new IllegalArgumentException(
						"%s command requires at most %s %s".formatted(name(), this.maxArgs, arguments(this.maxArgs)));
			}
		}
	}

	private String arguments(int count) {
		return count == 1 ? "argument" : "arguments";
	}

}
