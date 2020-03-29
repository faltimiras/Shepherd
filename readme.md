# Shepherd

Group elements according streaming rules executed asynchronously or on sliding or tumbling time windows.

![Catalan shepherd dog AKA: gos d'atura](gosdatura.jpg)

## Getting Started

Define your keyExtractor and your first rule (or use predetermined):
- Define a KeyExtractor to identify objects must be group. 
- Define a Rule

Then just create an instance of Shepherd

```
Shepherd shepherd = Shepherd.create()
    .basic(
        new FixedKeyExtractor(), 
        listCollector,
        new AccumulateNRule(2))
    .build();

shepherd.add(1);
shepherd.add(2);
shepherd.add(2);
...
```

This stupid piece of code will just accumulate integers until 2, after second one is processed, values are available on resultPool.


**Key Extractor**

Objects grouped must have same key.
Key can be provided externally 

```
shepherd.add("the-key", 2);
```

Or via a java functional interface Function implementation


SimpleKeyExtractor and FixedKeyExtractor are already coded. Useful for simple cases, but common.

**Rule**

Decides when an new element must be accumulated or to a group or it has to be "released". 
Rules are checked when a new element is added to shepherd instance.

```
public interface Rule<V, S> {
	RuleResult canClose(Metadata metadata, V value, LazyValue<?, V, S> lazyValues);
}
```

- **metadata** keeps metadata for this still-open-group identified by they key.
- **v** is the new element that has been added.
- **lazyValues** give access to the rule to get already grouped elements.

- **RuleResult** tells to shepherd what has to be done with the new element added.
3 actions can be defined: **Append** (or not) the value just added, **Discard** stored until this moment, **Close** the group and "released".
RuleResult has a bunch of cool methods to build RuleResults easily.

There are 6 already coded simple rules to cover simple cases: [Streaming rules](https://github.com/faltimiras/Shepherd/tree/master/src/main/java/cat/altimiras/shepherd/rules/streaming)

**Callback**

It is the way to get brand new groups created.

Callback it is just a Consumer Java functional interface.

There is a simple already coded Callback that accumulates results: ListCollector

WARNING: slow callback codes impact on shepherd performance and accuracy, consider to execute them in another thread.

**Windows**

A part of streaming Rules that are evaluated when a new element is added, groups can be closed when a time window expires.

Decide if window must be closed or not it is decided by a RuleWindow that you can implement.

 ```
RuleResult canClose(Metadata metadata, LazyValue<?, V, S> lazyValues);
 ```

This rule is checked against every key every some time (configurable), to close or not groups according time.

By default some window rules are in place and to abstract class ready to extend to support easily **Sliding** and **Tumbling** windows. Nice explanation about them on [kafka stream](https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#windowing) documentation.
Extend TumblingWindowBaseRule or SlidingWindowBaseRule to take advantage of their capabilities.
 
 ```
Shepherd shepherd = Shepherd.create()
    .basic(
        new SimpleKeyExtractor(),
        listCollector,
        new NoDuplicatesRule())
    .withWindow(
        Duration.ofMillis(5000), 
        new DiscardAllExpiredRule()
    ).build();
 ```
This piece of code removes duplicates during 5s windows. Basically first appearance is propagated, during next 5s, all appearances of it are silently removed.

**RuleExecutor**

Streaming rules are executed by default one after the other stopping at the first one that canClose, if no one of them can close, the storage is updated according last rule executed.

This behaviour can be changed providing a custom implementation of RuleExecutor

```
RuleResult<S> execute(final Metadata metadata, final V newValue, LazyValue lazyValues, List<Rule<V, S>> rules);
```

 ```
 Shepherd shepherd = Shepherd.create()
     .basic(...)
     .withRuleExecutor(myRuleExecutor)
     .withWindow(
         Duration.ofMillis(5000), 
         new DiscardAllExpiredRule())
     ).build();
 ```

Every group of elements has his own metadata instance, all rules shares and can update it.

**Parallelism**

Shepherds by default uses only one thread to execute streaming rules and to check opened windows. If this is not enough to handle the load, more threads can be added to parallelize the workload.

 ```
 Shepherd shepherd = Shepherd.create()
     .basic(...)
     .threads(5)
     ).build();
 ```


**Monitoring**

[Dropwizard metrics](https://metrics.dropwizard.io/) can be plugged to Shepherd.

 ```
import com.codahale.metrics.MetricRegistry

MetricRegistry metrics= ...

Shepherd shepherd = Shepherd.create()
    .basic(...)
    .withMonitoring(metrics)
 ```

Shepherd will add the avg time executing streaming rules, the avg time closing windows and the number of pending elements in the internal queue

**Storage**

Shepherd by default store "under construction" groups in memory, nevertheless it is configurable and storage can be totally custom via implementing ValuesStorage interface

There are 2 alternatively value storages ready to use: [Redis](https://redis.io/) and Files system.

Check examples on the tests.

**Sync mode**

Shepherd can be started in sync mode, then thread adding the new elements it evaluates also the streaming rules.
On sync mode, windows are not closed, it must be done manually calling shepherd.checkWindows().

Useful for testing.
 
## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Ferran Altimiras** - [faltimiras](https://github.com/faltimiras)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details