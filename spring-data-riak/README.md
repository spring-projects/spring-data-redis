# Spring Data support for Riak

The spring-data-riak module strives to make working with Riak painless by providing
the developer several different ways to easily access or store data using the Riak
Key/Value store.

## Recent Changes:

* 12/20/2010: AsyncRiakTemplate and Groovy DSL

### Groovy DSL

One cool new feature just added is a Groovy DSL for data access using SDKV/Riak:

    def riak = new RiakBuilder(riakTemplate)
    def result = null

    riak.set(bucket: "test", key: "test", qos: [dw: "all"], value: obj, wait: 3000L) {
      completed(when: { it.integer == 12 }) { result = it.test }
      completed { result = "otherwise" }

      failed { it.printStackTrace() }
    }

The Groovy DSL will respond to the following methods:

* set
* setAsBytes
* put
* get
* getAsBytes
* getAsType
* containsKey
* delete
* each

Each completed or failed closure can be accompanied by a "guard" closure. For example,
to process an entry differently, based on the type:

    riak.get(bucket: "test", key: "test") {
      completed(when: { it instanceof Map }) { processMap(it) }
      completed(when: { it instanceof String }) { processString(it) }
      completed(when: { it instanceof byte[] }) { processBytes(it) }
      completed { result = "otherwise" }

      failed { it.printStackTrace() }
    }

You can nest them, of course. To insert data and then delete all keys from a bucket:

    riak {
      put(bucket: "test", value: [test: "value 1"])
      put(bucket: "test", value: [test: "value 2"])
      put(bucket: "test", value: [test: "value 3"])

      each(bucket: "test") {
        completed { v, meta ->
          delete(bucket: meta.bucket, key: meta.key)
        }
        failed { it.printStackTrace() }
      }
    }


Some things to note here:

* The Groovy DSL utilizes the new AsyncRiakTemplate, so all closure calls happen
  asynchronously. By default, the operation will block indefinitely. To not block at all
  and continue on immediately, set the `wait` to `0`. To block until a specified timeout,
  set the `wait` to the number of milliseconds to wait for the operation to complete before
  timing out and throwing an exception.
* Callbacks are defined as either `completed` or `failed` closures. In addition to the
  closure, you can define a "guard" closure, which is called before the main closure and
  should return non-null or Boolean `true` if the closure should be executed or null or
  Boolean `false` if the closure is to be skipped. This functionality is inspired by the
  use of (the guard expression in Erlang case statements)[http://en.wikibooks.org/wiki/Erlang_Programming/guards].