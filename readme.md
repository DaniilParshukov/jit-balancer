
# Overview

Imagine that you are a runtime engineer of some Virtual Machine. This VM supports
advanced Just-In-Time compilation techniques: level 1 JIT is very fast but produces suboptimal
machine code, level 2 JIT is the best but requires a lot of time to do the compilation.

Your job is to implement a concurrent protocol that allows existing VM to use
JIT compiler in the most efficient way. Unfortunately, as in any real life task, 
"the most efficient" is vague concept:
- Some users expect that their methods run full-speed (they minimize "hot path execution time" and do not care about latency)
- Some users expect that their programs run full-speed (they minimize "wall-clock time" and do not care about CPU usage)
- Some users expect their programs to be energy efficient to pay less for cloud hosting (they minimize "used CPU cycles")
- Some users expect their programs are memory-efficient (they minimize "process RAM footprint")
- Some users expect that VM does not waste CPU (compilation of the same method twice is not allowed)
- Some users expect that VM does not overload target system (number of concurrent compilations is limited by some N)
- ... and many more definitions of efficiency

As you can see, there is no single "right" neither "best" solution for this task, you always have a trade-off.
Different levels of difficulty for this problem define some "non-correctness" constraints for your implementation, e.g.
"if this method was invoked 1 million times, it was scheduled for level 2 JIT compilation". Such constraints
will enforce you to:
- implement more complicated concurrent solution
- spend more time designing, debugging and testing it
- question yourself, if your design is "better" than previous

Indeed, in many cases, new design will be worse in terms of performance, latency or maintainability.
Please, document your final design in javadoc or markdown and explicitly state its weak points. It would be really cool if
you point out which particular constraint could be removed or modified to make your implementation
"better". **This is the main part** of task which is checked by your reviewers (teaching assistants, TAs).

We would like to teach you:
- how to spot "too strict" requirements for concurrent systems and avoid excessive complexity
- how to implement "very strict" requirements correctly if you can not change them

**The second important part** checked by TA is your code quality (documentation, modularity, additional testing). Be aware that almost any real concurrent system have bugs and performance corner cases. This task is covered
with plenty of auto-tests but passing all of them does not guarantee your solution is bug-free. Please, write additional tests for your
design. Please, add relevant `assert`s everywhere. Please, follow your TA recommendations. TAs are humans, they could miss
some bugs in your implementation. But if they do not, they have full right to ask you to write regression test first.

**Do not rush, think first**. Elegant, compact, easy-to-understand solutions exist for level 1 and level 2. If you are writing
a lot of code, be aware that TA will ask if this protocol really fits all the required constraints. Less you write -- less
you need to prove. **Do not hesitate to contact your TA** if you are stuck with this problem. It will not affect your score
(actually, it could even improve your total grade).

## Getting started

- Read javadoc for all classes in `org.nsu.syspro.parprog.external` package
- Inspect all samples in `org.nsu.syspro.parprog.examples` package
- Run all unit tests with dummy `org.nsu.syspro.parprog.solution.SolutionThread`
- Copy-paste some sample code into `SolutionThread` and re-run tests.

## Acceptance criteria

We define some constraint by using 
- `short-constraint-name`: description

notation. There are plenty of tests for every constraint. Passing such tests 
**does not imply** that your solution fits to the constraint. You must be ready to **prove it** 
informally, but convincing enough for your TA.

Hint: providing specially-designed tests with custom scheduling patterns is the most 
convincing way to pass this task.

### Level 1 (easy)

- `Same-method-correctness`: `UserThread.executeMethod(id)` executes specified method
- `Same-thread-correctness`: `UserThread.executeMethod(id)` executes specified method in the same thread
- `No-side-effect-correctness`: `UserThread.executeMethod(id)` does not triggered execution of other unexpected methods
- `Multithread-1-correctness`: several threads request `UserThread.executeMethod(id)` and eventually all requests 
are finished
- `Multithread-many-correctness`: several threads request `UserThread.executeMethod(id1,id2...idN)` and eventually all requests
  are finished 
- `Recursion-correctness`: `ExecutionEngine.*(id1)` cause invocation of `UserThread.executeMethod(id2)`, everything eventually finishes
- `Per-thread-monotonicity`: if `Thread A` executed `CompiledMethod(id)` which was produced by JIT level `x` then all
executions of the same method in the same thread will execute same or higher level of optimization. Clarification: yes, 
it is correct to compile the same method several times and use different "function pointers". The requirement enforces
"user-visible performance" and does not forbid complicated caching algorithms.
- `Eventual-per-thread-progress-1`: if `UserThread.executeMethod(id)` was invoked `10_000` times by `Thread A`, at least one invocation
inside this thread was using code produced level 1 or level 2 JIT.
- `Weak-global-caching`: denote `t1` the moment of time when `Thread A` finished `UserThread.executeMethod(id)`
  produced by level `x` JIT. 80% of all invocations (in all threads) of this method that start after `t1` are using same or higher level of optimization.

### Level 2 (medium)

- All Level 1 constraints
- `Heavy-compilation-offloading`: there is no `UserThread` that ever execute `compile_l2`
- `Eventual-per-thread-progress-2`: if `UserThread.executeMethod(id)` was invoked `100_000` times by `Thread A` as `CompiledMethod(id)`, 
at least one invocation inside this thread was using code produced by level 2 JIT.

### Level 3 (hard)

#### Warming-up
 
- All Level 1 and Level 2 constraints
- `Thread-bound-compilation`: at any moment of time, number of concurrently executed `compile_l1`/`compile_l2` is limited by
  `UserThread.compilationThreadBound` which is guaranteed to be `>= 2` and provided as `UserThread` constructor parameter
- `CPU-bound-compilation`: any method `id` was `compile_l1(id)` no more than twice, any method `id` was `compile_l2(id)` no more than once  

#### Preparing

- `Limited-threads`: during any execution number of different `UserThread`s is bounded by some constant (particular value 
is unknown by VM)
- `Limited-methods`:  during any execution number of different `MethodID`s is bounded by some constant (particular value
is unknown by VM) 

Clarification: `Limited-*` constraints help you to guarantee some interesting properties, e.g. "if `Thread A` uses 
thread-local thread-unsafe unbound `Set` for method caching, any lookup in this set will always finish in bounded time"

Clarification 2: these constraints do not require you to change the code, they only needed to help you with the next steps.

#### Going wild

The following constraint is very tricky, ensure you have working solution for all constraints above, document it, test it,
save it in separate class.

- `Weak-worst-case-latency`: in `Thread A` denote `t1` as moment of time when `UserThread.executeMethod(id)` was invoked,
denote `t2` as moment of time when `ExecutionEngine.interpret(id)` or `ExecutionEngine.execute` started. `t2 - t1` is
always a bounded time, even if compilation of this method takes arbitrary time. 

Clarification:
- If you have global method cache guarded by `ReadWriteLock` then method lookup is `bounded time` if and only if there is no
contention with `writeLock/writeUnlock`. In other words, many threads could safely read from the cache, it still counts as valid solution.
However, if there exists scenario when some thread attempts to read data but some other thread already owns `writeLock` 
then this in **not** "bound time". 
- The same reasoning (read-write contention on shared data is not "bound time", any actually blocking call is not "bound time")
applies for all other `java.util.concurrent` data structures that you are allowed to use in this task. In block 2 we will discuss such "guaranteed-progress" properties
as lock-free, wait-free and obstruction-free.

Solution hint 1: it looks like any contention will cause blocking which is prohibited. Looks like threads
should aggressively use "replication pattern", i.e. keep a lot of data in thread-local storage. Synchronizing this could 
be a headache, do not hesitate using simple blocking methods for threads that are not on "critical fast path".

Solution hint 2: contention is forbidden in the beginning of `executeMethod`, it seems OK to use blocking calls after method 
finished.

#### To the infinity and beyond (optional)

**THIS PART IS NOT REQUIRED TO FINISH LEVEL 3**

**THIS COULD TAKE MORE TIME THAN YOU EXPECT**

Consider the following constraint:
- `Strong-worst-case-latency`: in `Thread A` denote `t1` as moment of time when `UserThread.executeMethod(id)` was invoked,
denote `t2` as moment of time when `UserThread.executeMethod(id)` was finished. Assume execution of `MethoID(id)` takes `delta` nanoseconds.
`t2 - t1 - delta` is always a bounded time.

**Important:** this constraint contradicts some other constraints. Provide test with custom timings to prove this incompatibility on **any** implementation.
Decide which constraints it is better to "drop", motivate your choice, implement.

## Recommendations

If you have ideas on how to solve this task -- give it a try! For those who are in doubt, the following
ideas could be a good starting point.

- Start with naive implementations from examples. Understand which Level 1 constraints are the hardest ones.
- Consider using thread-unsafe collection as a cache of compiled methods. Keep this collection as thread-local data. Be cautious and do
not allow data races on it.
- Consider using thread-safe collection as a global cache of compiled methods. Beware of race conditions and
other troubles while trying to keep "in sync" local and global cache. If you are going to drop local caches in your design,
do not forget to document this design decision, possible drawbacks and positive consequences.
- Consider using dedicated threads for compilation tasks. Caveat: `Eventual-per-thread-progress-1` suggests that
at some point you need to "slow down" your `UserThread` from interpreting the same method too many times. You may need
`Future` or `CountDownLatch` or some custom message passing protocol.
- Consider using `Executor`s/`ThreadPool`s for compiler threads.
- Consider using producer-consumer pattern to distribute compilation requests.
- Consider using advanced concurrent structures for more efficient implementation (e.g. guard read-mostly data by 
`ReadWriteLock` instead of `ReentrantLock`)
- If you are brave enough to go Level 3, remember
  - thread-local cache will not ever grow to infinity, thanks to `Limited-*` constraints. All operations on such thread-local data could be treated as O(1).
  - global cache could be guarded by `ReadWriteLock` or `ReentrantLock` but remember that current thread 
 may block until other thread finishes the update, overhead of blocking is **not** "bounded time". However, using 
`check-and-retreat` (`Lock.tryLock`) considered as "bounded time".  
