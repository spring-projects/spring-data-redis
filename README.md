[![Spring Data Redis](https://spring.io/badges/spring-data-redis/ga.svg)](http://projects.spring.io/spring-data-redis/#quick-start)
[![Spring Data Redis](https://spring.io/badges/spring-data-redis/snapshot.svg)](http://projects.spring.io/spring-data-redis/#quick-start)

Spring Data Redis
=======================

The primary goal of the [Spring Data](http://projects.spring.io/spring-data/) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.
This modules provides integration with the [Redis](http://redis.io/) store. 

# Docs

You can find out more details from the [user documentation](http://docs.spring.io/spring-data/data-redis/docs/current/reference/html/) or by browsing the [javadocs](http://docs.spring.io/spring-data/data-redis/docs/current/api/).

# Examples

For examples on using the Spring Data Key Value, see the dedicated project, also available on [GitHub](https://github.com/spring-projects/spring-data-keyvalue-examples)

# Artifacts

## Maven configuration

Add the Maven dependency:

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-redis</artifactId>
  <version>${version}.RELEASE</version>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-redis</artifactId>
  <version>${version}.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>spring-libs-snapshot</id>
  <name>Spring Snapshot Repository</name>
  <url>http://repo.spring.io/libs-snapshot</url>
</repository>
```

## Gradle 

```groovy
repositories {
   maven { url "http://repo.spring.io/libs-milestone" }
   maven { url "http://repo.spring.io/libs-snapshot" }
}

// used for nightly builds
dependencies {
   compile "org.springframework.data:spring-data-redis:${version}"
}
```

# Usage (for the impatient)

* Configure the Redis connector to use (here [jedis](https://github.com/xetorthio/jedis)):

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:p="http://www.springframework.org/schema/p"
  xsi:schemaLocation="
  http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">
  
  <bean id="jedisFactory" class="org.springframework.data.redis.connection.jedis.JedisConnectionFactory"/>
  
  <bean id="redisTemplate" class="org.springframework.data.redis.core.RedisTemplate"
      p:connection-factory="jedisFactory"/>
</beans>
```

* Use `RedisTemplate` to interact with the Redis store:

```java
String random = template.randomKey();
template.set(random, new Person("John", "Smith"));
```

* Use Redis 'views' to execute specific operations based on the underlying Redis type:

```java
ListOperations<String, Person> listOps = template.listOps();
listOps.rightPush(random, new Person("Jane", "Smith"));
List<Person> peopleOnSecondFloor = listOps.range("users:floor:2", 0, -1);
```

# Building

Spring Data Redis uses Maven as its build system. 
Running the tests requires you to have a RedisServer running at its default port. Using the `-D runLongTests=true` option executes additional Pub/Sub test.

```bash
    mvn clean install
```

You can alternatively use the provided `Makefile` which runs the build plus downloads and spins up the following environment:

* 1 Single Node
* HA Redis (1 Master, 2 Slaves, 3 Sentinels).
* Redis Cluster (3 Masters, 1 Slave) 

```bash
    make test
```

# Contributing

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Stackoverflow.  Please help out on the [spring-data-redis](http://stackoverflow.com/questions/tagged/spring-data-redis) tag by responding to questions and joining the debate.
* Create [JIRA](https://jira.spring.io/browse/DATAREDIS) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Watch for upcoming articles on Spring by [subscribing](https://spring.io/blog) to spring.io.

Before we accept a non-trivial patch or pull request we will need you to [sign the Contributor License Agreement](https://cla.pivotal.io/sign/spring). Signing the contributor’s agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do. If you forget to do so, you'll be reminded when you submit a pull request.

Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, read the Spring Framework [contributor guidelines] (https://github.com/spring-projects/spring-framework/blob/master/CONTRIBUTING.md).

# Staying in touch

Follow the project team ([@SpringData](http://twitter.com/springdata)) on Twitter. In-depth articles can be
found at the Spring [team blog](https://spring.io/blog), and releases are announced via our [news feed](https://spring.io/blog/category/news).
