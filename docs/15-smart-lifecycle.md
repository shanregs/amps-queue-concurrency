# SmartLifecycle — Spring Bean Lifecycle, ApplicationContext, and Enterprise Patterns

> Covers: complete Spring bean lifecycle, ApplicationContext refresh phases,
> BeanPostProcessor, initialization callbacks, context events, `SmartLifecycle`
> theory, phase ordering, graceful shutdown, and how this project uses the interface.

---

## 1. Spring Bean Lifecycle — The Seven Stages

Every Spring-managed bean passes through a fixed sequence from the moment the container
decides to create it until the context is destroyed. Understanding this sequence is the
foundation for knowing **when** your code runs and why `@PostConstruct` is not enough
for long-running components.

### 1.1 The Seven Stages at a Glance

```
  ApplicationContext.refresh()
        │
        │  [Stage 1] Instantiation
        │    └── Constructor / factory method called
        │        Bean object exists; no dependencies yet
        │
        │  [Stage 2] Dependency Injection
        │    └── @Autowired fields / setter methods populated
        │        Constructor-injected beans are done by Stage 1
        │
        │  [Stage 3] Aware Callbacks
        │    └── BeanNameAware.setBeanName()
        │        BeanFactoryAware.setBeanFactory()
        │        ApplicationContextAware.setApplicationContext()
        │
        │  [Stage 4] BeanPostProcessor — Before Initialization
        │    └── postProcessBeforeInitialization() called on every BPP
        │        Example: @Autowired injection (AutowiredAnnotationBeanPostProcessor)
        │
        │  [Stage 5] Initialization Callbacks
        │    └── @PostConstruct method
        │        InitializingBean.afterPropertiesSet()
        │        @Bean(initMethod = "init")
        │
        │  [Stage 6] BeanPostProcessor — After Initialization   ← AOP proxies created HERE
        │    └── postProcessAfterInitialization() called on every BPP
        │        AopProxyCreator wraps bean in CGLIB/JDK proxy
        │        Bean is replaced in context by its proxy
        │
        │  [Stage 7] Bean In Use
        │    └── Available via ApplicationContext.getBean()
        │        Dependency-injected into other beans
        │
        ▼
  Context running...
        │
        │  [Destruction Phase]
        │    └── @PreDestroy method
        │        DisposableBean.destroy()
        │        @Bean(destroyMethod = "cleanup")
        ▼
  Context closed
```

### 1.2 Stage 1 — Instantiation

Spring creates the bean instance. For `@Component` / `@Service` / `@Repository` classes
this is a plain constructor call. For `@Bean` methods it is a method invocation.

```java
@Component
public class OrderService {

    private final PaymentClient paymentClient;

    // Constructor injection — dependency resolved before this call returns
    public OrderService(PaymentClient paymentClient) {
        this.paymentClient = paymentClient;
        // paymentClient is guaranteed non-null here (constructor injection)
        // But: ApplicationContext is NOT available yet — do not call Spring APIs here
    }
}
```

> **Rule**: constructors should only assign fields. Never call `applicationContext.getBean()`,
> never start threads, never open network connections.

### 1.3 Stage 2 — Dependency Injection

Field and setter `@Autowired` are resolved after the object is constructed.

```java
@Component
public class ReportService {

    @Autowired                          // injected after constructor
    private EmailClient emailClient;

    @Autowired
    public void setMetrics(MeterRegistry registry) {  // setter injection
        this.registry = registry;
    }
}
```

Constructor injection is preferred because:
- The bean is immutable (`final` fields)
- Circular dependencies are detected at startup, not at runtime
- All dependencies are visible in one place

### 1.4 Stage 3 — Aware Callbacks

`*Aware` interfaces let a bean receive references to Spring infrastructure objects.

```java
@Component
public class BeanInspector
        implements BeanNameAware, ApplicationContextAware {

    private String beanName;
    private ApplicationContext context;

    @Override
    public void setBeanName(String name) {
        this.beanName = name;           // called before @PostConstruct
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) {
        this.context = ctx;             // called before @PostConstruct
    }
}
```

| Aware Interface | What it injects |
|---|---|
| `BeanNameAware` | The bean's name in the context |
| `BeanFactoryAware` | The owning `BeanFactory` |
| `ApplicationContextAware` | The `ApplicationContext` |
| `EnvironmentAware` | The `Environment` (profiles, properties) |
| `MessageSourceAware` | i18n message source |
| `ResourceLoaderAware` | Resource loading API |

> Prefer constructor injection over `ApplicationContextAware` — it is cleaner and testable.
> Use `ApplicationContextAware` only for framework-level infrastructure beans.

### 1.5 Stage 4 — BeanPostProcessor Before Initialization

`BeanPostProcessor` (BPP) is a hook that Spring calls around every bean's initialization.
The *before* callback fires after Aware callbacks but before `@PostConstruct`.

```java
public interface BeanPostProcessor {
    // Stage 4 — before @PostConstruct / afterPropertiesSet
    Object postProcessBeforeInitialization(Object bean, String beanName);

    // Stage 6 — after @PostConstruct / afterPropertiesSet (AOP lives here)
    Object postProcessAfterInitialization(Object bean, String beanName);
}
```

Spring's own BPPs (registered automatically):

| BPP | What it does |
|---|---|
| `AutowiredAnnotationBeanPostProcessor` | Resolves `@Autowired`, `@Value`, `@Inject` |
| `CommonAnnotationBeanPostProcessor` | Handles `@PostConstruct`, `@PreDestroy`, `@Resource` |
| `PersistenceAnnotationBeanPostProcessor` | Injects `@PersistenceContext` / `@PersistenceUnit` |
| `AbstractAdvisorAutoProxyCreator` | Creates AOP proxies (Stage 6) |

### 1.6 Stage 5 — Initialization Callbacks

Three equivalent mechanisms, in the order Spring calls them:

```java
@Component
public class CacheWarmup implements InitializingBean {

    // Called first (Stage 5a)
    @PostConstruct
    public void init() {
        log.info("@PostConstruct — dependencies injected, context NOT yet fully ready");
        // Safe: access injected dependencies
        // Unsafe: publish events (multicaster not yet set up for all listeners)
        // Unsafe: start background threads that call other beans (context may be mid-init)
    }

    // Called second (Stage 5b)
    @Override
    public void afterPropertiesSet() {
        log.info("afterPropertiesSet — same timing as @PostConstruct");
    }

    // Called third if @Bean(initMethod = "customInit") is set (Stage 5c)
    public void customInit() {
        log.info("custom init method");
    }
}
```

**What @PostConstruct can safely do**:
- Validate configuration properties
- Build in-memory data structures from injected dependencies
- Register with a local registry

**What @PostConstruct must NOT do**:
- Start long-running threads that consume from a queue
- Connect to external systems that depend on other Spring beans being ready
- Publish `ApplicationEvent`s (event multicaster may not have all listeners yet)
- Call `context.getBean()` on beans not yet initialized (circular init risk)

### 1.7 Stage 6 — BeanPostProcessor After Initialization (AOP)

This is when Spring creates **AOP proxies**. The `postProcessAfterInitialization` return
value replaces the bean in the container. After this point, `context.getBean(OrderService.class)`
returns the CGLIB proxy, not the raw `OrderService` instance.

```
  Raw OrderService  ──[postProcessAfterInitialization]──►  CGLIB proxy (OrderService$$Enhancer)
                                                                │
                                                          @Transactional advice
                                                          @Cacheable advice
                                                          @Async advice
```

**Consequence for `@PostConstruct`**: when `@PostConstruct` runs (Stage 5), the AOP proxy
does not exist yet. Calling `this.someMethod()` inside `@PostConstruct` bypasses `@Transactional`
— the transaction interceptor is on the proxy, not on `this`.

```java
@Service
public class OrderService {

    @PostConstruct
    public void init() {
        createDefaultOrder();   // BUG: no @Transactional — proxy not created yet
    }

    @Transactional
    public void createDefaultOrder() { ... }
}

// Fix: inject self-proxy, or move initialization to ApplicationRunner / SmartLifecycle
```

### 1.8 Stage 7 — Bean In Use

After Stage 6 the bean is registered in the context, available via `getBean()` and
dependency-injected into other beans that need it.

### 1.9 Destruction Phase

When the context is closed (JVM shutdown hook, `context.close()`, or test teardown):

```java
@Component
public class ConnectionHolder implements DisposableBean {

    @PreDestroy              // called first
    public void preDestroy() {
        log.info("@PreDestroy — context still partially available");
    }

    @Override
    public void destroy() { // called second (DisposableBean)
        connection.close();
    }
    // @Bean(destroyMethod = "shutdown") would be called third
}
```

**Limitation of `@PreDestroy`**: it is called synchronously on the shutdown thread.
There is no timeout, no async drain, no phase ordering. All `@PreDestroy` methods run
**after** `SmartLifecycle.stop()` has completed for all phases — so by this point your
background workers should already be stopped.

---

## 2. ApplicationContext Lifecycle — Inside `refresh()`

`ApplicationContext.refresh()` is the largest single method in the Spring framework.
Everything from bean definition reading to SmartLifecycle startup happens inside it.

### 2.1 The Full `refresh()` Sequence

```
AbstractApplicationContext.refresh()
│
├── prepareRefresh()
│     └── Set start timestamp, mark context as active
│         Validate required properties (PropertySourcesPropertyResolver)
│         Initialize PropertySources (e.g. load application.yaml)
│
├── obtainFreshBeanFactory()
│     └── Create DefaultListableBeanFactory
│         Read bean definitions:
│           @Configuration classes → ConfigurationClassParser
│           @ComponentScan → ClassPathBeanDefinitionScanner  [SCAN happens here]
│           XML → XmlBeanDefinitionReader
│           @Bean methods registered as BeanDefinitions
│         Result: BeanDefinitionRegistry filled, NO beans instantiated yet
│
├── prepareBeanFactory(beanFactory)
│     └── Register built-in BPPs:
│           ApplicationContextAwareProcessor
│           ApplicationListenerDetector
│         Register built-in resolvable dependencies:
│           BeanFactory, ResourceLoader, ApplicationEventPublisher, ApplicationContext
│
├── postProcessBeanFactory(beanFactory)
│     └── Subclass hook (e.g. ServletWebServerApplicationContext registers
│         web-specific BPPs and scopes)
│
├── invokeBeanFactoryPostProcessors(beanFactory)     ← @Configuration processed HERE
│     └── Call BeanDefinitionRegistryPostProcessor first (e.g. ConfigurationClassPostProcessor)
│         → @Configuration classes parsed: @Bean, @Import, @ComponentScan re-scanned
│         → @PropertySource files loaded, @Value placeholders registered
│         Then call BeanFactoryPostProcessor
│         → PropertySourcesPlaceholderConfigurer resolves ${...} in bean definitions
│
├── registerBeanPostProcessors(beanFactory)
│     └── Instantiate and register all BeanPostProcessor beans
│         (AutowiredAnnotationBPP, CommonAnnotationBPP, etc.)
│         BPPs are created here — before normal beans
│
├── initMessageSource()                              ← i18n
│
├── initApplicationEventMulticaster()               ← event system ready
│
├── onRefresh()                                      ← SUBCLASS HOOK
│     └── SpringBoot WebServer: Tomcat/Netty starts here and begins listening
│         (HTTP server is UP before any singleton beans are initialized!)
│
├── registerListeners()
│     └── Register ApplicationListener beans found in context
│         Post any early ApplicationEvents that were collected
│
├── finishBeanFactoryInitialization(beanFactory)    ← ALL SINGLETONS CREATED HERE
│     └── Freeze bean definitions (no more registration after this)
│         preInstantiateSingletons():
│           For each non-lazy singleton bean definition:
│             └── getBean(beanName)
│                   → instantiate (constructor)
│                   → inject dependencies (@Autowired)
│                   → Aware callbacks
│                   → BPP.postProcessBeforeInitialization
│                   → @PostConstruct / afterPropertiesSet   ← YOUR INIT CODE RUNS HERE
│                   → BPP.postProcessAfterInitialization    ← AOP PROXY CREATED HERE
│
└── finishRefresh()                                 ← CONTEXT FULLY READY
      └── clearResourceCaches()
          initLifecycleProcessor()
          lifecycleProcessor.onRefresh()            ← SmartLifecycle.start() CALLED HERE
          publishEvent(ContextRefreshedEvent)       ← @EventListener fires AFTER lifecycle start
          LiveBeansView.registerApplicationContext()
```

### 2.2 The Critical Ordering Insight

```
finishBeanFactoryInitialization()   @PostConstruct runs here
        │
        │  All singleton beans fully constructed and initialized
        │  AOP proxies in place
        │
        ▼
finishRefresh()
        │
        ├── SmartLifecycle.start() called (phase ordered)   ← LONG-RUNNING BEANS START HERE
        │
        └── ContextRefreshedEvent published                 ← @EventListener fires here
```

This explains the original problem statement:

| Mechanism | Runs when | Context state |
|---|---|---|
| Constructor | Mid-refresh, before any singleton is ready | Partial — deps may not be injected yet |
| `@PostConstruct` | End of `finishBeanFactoryInitialization()` | All singletons ready, but no lifecycle started, no events fired |
| `@EventListener(ContextRefreshedEvent)` | End of `finishRefresh()` | Full context ready — but called on event thread, no phase ordering |
| `SmartLifecycle.start()` | `finishRefresh()` → `lifecycleProcessor.onRefresh()` | Full context ready, **phase ordered** |

### 2.3 Context Events — Full Timeline

Spring publishes `ApplicationEvent`s at each major context transition:

```
  new AnnotationConfigApplicationContext(...)
        │
        └── refresh()
              │
              ▼
        ContextRefreshedEvent          — context fully started (every refresh)
              │
  context.start()  (manual)
              │
              ▼
        ContextStartedEvent            — Lifecycle beans started
              │
  context.stop()  (manual)
              │
              ▼
        ContextStoppedEvent            — Lifecycle beans stopped (restartable)
              │
  context.close() / JVM shutdown
              │
              ▼
        ContextClosedEvent             — context destroyed (not restartable)
```

### 2.4 Reacting to Context Events

```java
@Component
public class StartupAudit {

    // Fires after SmartLifecycle.start() — context fully operational
    @EventListener(ContextRefreshedEvent.class)
    public void onRefreshed(ContextRefreshedEvent event) {
        log.info("Context refreshed: {}", event.getApplicationContext().getId());
        // Safe: all beans ready, all SmartLifecycle beans already started
    }

    // Fires when context.close() is called — BEFORE @PreDestroy, AFTER SmartLifecycle.stop()
    @EventListener(ContextClosedEvent.class)
    public void onClosed(ContextClosedEvent event) {
        log.info("Context closing — lifecycle beans already stopped");
    }
}
```

> **Important**: `ContextRefreshedEvent` fires on **every** `refresh()` call.
> In a Spring Boot app that is `refresh()`ed multiple times (e.g. dev tools restart),
> guard with a flag or use `ApplicationReadyEvent` instead.

```java
// Spring Boot specific — fires ONCE after embedded server is ready for traffic
@EventListener(ApplicationReadyEvent.class)
public void onReady() {
    log.info("Application ready — server accepting requests");
}
```

---

## 3. Key Extension Points — When to Use Each

### 3.1 BeanFactoryPostProcessor — Modify Definitions Before Instantiation

Runs during `invokeBeanFactoryPostProcessors()` — **before any bean is instantiated**.
Use to:
- Add or modify bean definitions programmatically
- Resolve property placeholders (`PropertySourcesPlaceholderConfigurer`)
- Conditional bean registration based on environment

```java
@Component
public class FeatureFlagBeanRegistrar implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        if (featureFlagEnabled("new-payment-processor")) {
            registry.registerBeanDefinition("paymentProcessor",
                BeanDefinitionBuilder.genericBeanDefinition(NewPaymentProcessor.class)
                    .getBeanDefinition());
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {}
}
```

### 3.2 BeanPostProcessor — Wrap Every Bean

Runs around **each bean's initialization** (Stages 4 and 6). Use to:
- Add cross-cutting behavior (this is how Spring AOP works)
- Validate bean state after initialization
- Apply custom annotations

```java
@Component
public class MetricsInstrumentingBPP implements BeanPostProcessor {

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean.getClass().isAnnotationPresent(Instrumented.class)) {
            return Proxy.newProxyInstance(                // wrap in metrics proxy
                bean.getClass().getClassLoader(),
                bean.getClass().getInterfaces(),
                new MetricsInvocationHandler(bean, meterRegistry)
            );
        }
        return bean;
    }
}
```

### 3.3 InitializingBean vs @PostConstruct vs @Bean(initMethod)

All three run at Stage 5. Prefer `@PostConstruct` for application code (no Spring API
coupling). Use `InitializingBean` when you need guaranteed ordering relative to other
init mechanisms. Use `@Bean(initMethod)` for third-party classes you can't annotate.

```java
// All three are equivalent — Spring calls them in this order if all are present:
// 1. @PostConstruct
// 2. afterPropertiesSet()
// 3. custom initMethod

@Component
public class DataLoader implements InitializingBean {

    @PostConstruct
    public void postConstruct() { /* 1st */ }

    @Override
    public void afterPropertiesSet() { /* 2nd */ }
}

@Configuration
public class ThirdPartyConfig {
    @Bean(initMethod = "open", destroyMethod = "close")
    public LegacyConnectionPool pool() { return new LegacyConnectionPool(); }
}
```

### 3.4 DisposableBean vs @PreDestroy vs @Bean(destroyMethod)

Same pattern as init — three equivalent mechanisms called in order:
1. `@PreDestroy`
2. `destroy()` (DisposableBean)
3. custom destroyMethod

All run on the Spring shutdown thread **after SmartLifecycle.stop()** for all phases
has completed. They are synchronous with no timeout.

### 3.5 ApplicationListener vs @EventListener

```java
// Older style — type-safe, but requires implementing an interface
@Component
public class AuditListener implements ApplicationListener<OrderCreatedEvent> {
    @Override
    public void onApplicationEvent(OrderCreatedEvent event) { ... }
}

// Modern style — any method, any visibility, conditional
@Component
public class AuditListener {

    @EventListener
    public void on(OrderCreatedEvent event) { ... }

    @EventListener(condition = "#event.amount > 10000")
    public void onLargeOrder(OrderCreatedEvent event) { ... }

    @Async
    @EventListener
    public void onAsync(OrderCreatedEvent event) { ... }  // non-blocking
}
```

Publishing events:

```java
@Service
public class OrderService {

    private final ApplicationEventPublisher publisher;

    public void placeOrder(Order order) {
        repository.save(order);
        publisher.publishEvent(new OrderCreatedEvent(this, order));   // synchronous by default
    }
}
```

### 3.6 ApplicationRunner / CommandLineRunner

Run **once**, after the full context is ready and the embedded server is listening.
Good for CLI tools, one-time DB seed, startup validation.

```java
@Component
@Order(1)
public class SchemaValidator implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // Context fully ready, server up, SmartLifecycle beans running
        // Safe to call any bean, any service
        validateDatabaseSchema();
    }
}
```

---

## 4. Why @PostConstruct Is Too Early for Long-Running Beans

Pulling the timeline together, here is exactly what "too early" means:

```
finishBeanFactoryInitialization()
  └── OrderService instantiated
        └── @PostConstruct init() called       ← YOU ARE HERE
              │
              │  At this moment:
              │  ✓ All @Autowired deps are injected
              │  ✓ @Value properties resolved
              │  ✗ AOP proxy NOT created yet (Stage 6 not done)
              │  ✗ Other singleton beans may still be mid-init
              │  ✗ No SmartLifecycle bean has started
              │  ✗ No ApplicationEvent has been published
              │  ✗ ContextRefreshedEvent not fired yet
              │  ✗ ApplicationReadyEvent not fired yet
              │  ✗ Embedded server may not be listening yet
```

**Concrete failure scenario**: a background subscriber thread started in `@PostConstruct`
that calls `orderRepository.save()` — another bean. If `orderRepository`'s own
`@PostConstruct` (e.g., schema validation) has not run yet because bean init order is
undefined, the call fails with a null pointer or uninitialized state.

```java
// BROKEN: background thread starts before context is fully ready
@PostConstruct
public void startConsuming() {
    Thread.ofVirtual().start(() -> {
        while (running) {
            Message msg = client.receive();
            orderRepository.save(msg.toOrder());  // orderRepository may not be ready!
        }
    });
}

// CORRECT: SmartLifecycle.start() — called after ALL singletons are initialized
@Override
public void start() {
    if (!running.compareAndSet(false, true)) return;
    Thread.ofVirtual().start(() -> {
        while (running.get()) {
            Message msg = client.receive();
            orderRepository.save(msg.toOrder());  // safe — all beans ready
        }
    });
}
```

**The three things `SmartLifecycle.start()` gives you that `@PostConstruct` does not**:

| Guarantee | `@PostConstruct` | `SmartLifecycle.start()` |
|---|---|---|
| All singletons initialized | No — order undefined | Yes — runs after `finishBeanFactoryInitialization` |
| AOP proxies in place | No | Yes |
| Ordered relative to other beans | No | Yes (phase) |
| Async graceful shutdown | No | Yes (stop callback) |

---

## 5. The Lifecycle Hierarchy

```
java.lang.Object
  └── org.springframework.context.Lifecycle              (start / stop / isRunning)
        └── org.springframework.context.SmartLifecycle  (+ phase / isAutoStartup / stop(Runnable))
```

### 5.1 `Lifecycle` interface

```java
public interface Lifecycle {
    void start();
    void stop();
    boolean isRunning();
}
```

Beans implementing `Lifecycle` are started by `LifecycleProcessor` during `finishRefresh()`
when `context.start()` is called (or automatically for `SmartLifecycle` with `isAutoStartup()`).

**Limitation**: no ordering guarantee between plain `Lifecycle` beans.

### 5.2 `SmartLifecycle` interface

```java
public interface SmartLifecycle extends Lifecycle, Phased {

    // default = true — start automatically on context refresh
    default boolean isAutoStartup() { return true; }

    // graceful async stop: do cleanup, then call callback.run()
    void stop(Runnable callback);

    // phase: lower = starts first, stops last (default = Integer.MAX_VALUE)
    default int getPhase() { return DEFAULT_PHASE; }   // Integer.MAX_VALUE
}
```

Key upgrade over `Lifecycle`:

| Feature | `Lifecycle` | `SmartLifecycle` |
|---|---|---|
| Auto-start on refresh | No | Yes (`isAutoStartup`) |
| Phase ordering | No | Yes (`getPhase()`) |
| Async graceful stop | No | Yes (`stop(Runnable)`) |
| Stop timeout | No | Yes (configurable) |

---

## 6. Phase Ordering

Phase is an `int`. Spring starts beans **lowest phase first** and stops them **highest phase first**
(i.e., stop order is the reverse of start order).

```
          START order ──────────────────────────────────────────►
          ◄────────────────────────────────────────── STOP order

  Phase   MIN_VALUE  ...   -1     0     1  ...  MAX_VALUE-1  MAX_VALUE
  ──────────────────────────────────────────────────────────────────────
  Example  DB pool       Health  App   HTTP   Publisher    Subscriber
```

### Well-known convention in this project

| Bean | Phase | Rationale |
|---|---|---|
| `AmpsMessagePublisher` | `MAX_VALUE - 1` | Stops one step before subscriber so subscriber drains queue first |
| `MultiAmpsSubscriberPool` | `MAX_VALUE` (default) | Last to start, last to stop — keeps consuming until context is torn down |
| `SingleAmpsSubscriber` | `MAX_VALUE` (default) | Same |

> Rule of thumb: **infrastructure first, application last** on start;
> **application first, infrastructure last** on stop.

### Concrete phase ladder for enterprise apps

```
Phase              Component                  Reason
─────────────────────────────────────────────────────────────────────
Integer.MIN_VALUE  Schema migration (Flyway)  Must run before any bean touches DB
-1000              DataSource / pool          Opened before any service needs it
  0                Cache warmup               Populated before HTTP traffic
 100               Metrics / tracing          Ready before business beans
1000               Business services          Normal application beans
MAX_VALUE-100      Rate limiter replenisher   Near the top, stops before workers
MAX_VALUE-1        Publisher / producer       Stops before consumer drains
MAX_VALUE          Consumer / subscriber      Last running; drains the queue
```

---

## 7. The Stop Callback — Graceful Shutdown in Depth

```java
// Wrong — synchronous stop, no drain
@Override
public void stop() {
    running = false;
    client.disconnect();
}

// Right — async drain, then signal done
@Override
public void stop(Runnable callback) {
    running = false;
    executor.submit(() -> {
        semaphore.acquireUninterruptibly(maxConcurrency); // wait for in-flight
        client.disconnect();
        callback.run();  // ← MUST be called, or Spring hangs
    });
}
```

Spring's `LifecycleProcessor` calls `stop(Runnable)` and then waits up to
`spring.lifecycle.timeout-per-shutdown-phase` (default **30 s**) for `callback.run()`.
If the timeout expires, Spring proceeds without waiting — the callback is never a hard
prerequisite, just a best-effort drain window.

```yaml
# application.yaml
spring:
  lifecycle:
    timeout-per-shutdown-phase: 60s   # extend if messages take longer to drain
```

### State machine for a SmartLifecycle bean

```
        Context refresh
              │
              ▼
        ┌───────────┐   start()    ┌─────────┐
        │  STOPPED  │─────────────►│ RUNNING │
        └───────────┘              └────┬────┘
              ▲                         │ stop(Runnable)
              │                         ▼
              │                   ┌──────────┐
              │  callback.run()   │ DRAINING │
              └───────────────────┤          │
                                  └──────────┘
```

---

## 8. `isAutoStartup()` — Lazy / Conditional Startup

```java
@Override
public boolean isAutoStartup() {
    return environment.getProperty("amps.subscriber.auto-start", Boolean.class, true);
}
```

Return `false` to suppress automatic start on context refresh. The bean can then be
started manually:

```java
lifecycleProcessor.start(myBean);    // or
applicationContext.start();          // starts all Lifecycle beans
```

Use cases:
- Consumer that should only run in production, not in developer local runs
- Subscriber that waits for a feature flag before enabling
- Test harness that starts components on demand

---

## 9. Full Implementation Template

```java
import org.springframework.context.SmartLifecycle;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ExampleWorker implements SmartLifecycle {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int maxConcurrency;
    private final Semaphore semaphore;
    private ExecutorService executor;

    public ExampleWorker(@Value("${worker.max-concurrency:50}") int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        this.semaphore = new Semaphore(maxConcurrency);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;  // idempotent
        executor = Executors.newVirtualThreadPerTaskExecutor();
        connectAndSubscribe();
        log.info("ExampleWorker started, phase={}", getPhase());
    }

    @Override
    public void stop(Runnable callback) {
        if (!running.compareAndSet(true, false)) {
            callback.run();   // already stopped — still must call callback
            return;
        }
        Thread.ofVirtual().start(() -> {
            try {
                semaphore.acquireUninterruptibly(maxConcurrency);
                log.info("ExampleWorker drained all in-flight work");
            } finally {
                disconnect();
                callback.run();  // ← always called in finally
            }
        });
    }

    @Override
    public void stop() {
        stop(() -> {});
    }

    @Override
    public boolean isRunning() { return running.get(); }

    @Override
    public int getPhase() { return Integer.MAX_VALUE; }

    @Override
    public boolean isAutoStartup() { return true; }
}
```

---

## 10. How This Project Uses SmartLifecycle

### 10.1 `SingleAmpsSubscriber` (single-subscriber profile)

```
start()
  └── opens HAClient (AMPS TCP connection)
  └── haClient.subscribe(topic, handler)
  └── subscriber platform thread: for each msg → semaphore.acquire → VT submit

stop(Runnable)
  └── running = false                       (handler checks this)
  └── VT: semaphore.acquireUninterruptibly(maxConcurrency)  ← drain
  └── haClient.disconnect()
  └── callback.run()
```

### 10.2 `MultiAmpsSubscriberPool` (multi-subscriber / multi-jvm-subscriber)

```
start()
  └── for i in [0, subscriberCount):
        └── HAClient_i  connects to AMPS
        └── HAClient_i.subscribe(topic, handler_i)
        └── semaphore_i = new Semaphore(maxConcurrencyPerSubscriber)

stop(Runnable)
  └── running = false
  └── VT: for each semaphore_i → acquireUninterruptibly(maxConcurrencyPerSubscriber)
  └── for each client_i → disconnect()
  └── callback.run()
```

The per-subscriber semaphore design means one slow subscriber cannot starve others
during drain — each semaphore drains independently in parallel.

### 10.3 `AmpsMessagePublisher` (message-publisher profile)

```
phase = MAX_VALUE - 1    ← stops ONE phase before subscribers

start()
  └── sharedHaClient.connect()
  └── VT worker pool: publishMessages() loop with rateLimiter.acquire()

stop(Runnable)
  └── running = false               (workers check this flag)
  └── executor.shutdown()
  └── executor.awaitTermination(30, SECONDS)
  └── sharedHaClient.disconnect()
  └── callback.run()
```

Phase `MAX_VALUE - 1` ensures the publisher drains and stops **before** the subscriber
receives the stop signal. This prevents a scenario where the subscriber is stopped while
the publisher is still injecting messages.

---

## 11. SmartLifecycle vs Alternatives

| Mechanism | When to Use |
|---|---|
| Constructor | Assign injected dependencies only — never start work |
| `@PostConstruct` | Validate config, build in-memory structures, one-time setup |
| `InitializingBean` | Same as `@PostConstruct`, slightly more explicit |
| `ApplicationRunner` | Run once after server is ready: CLI tools, seed data |
| `@EventListener(ContextRefreshedEvent)` | React to context-ready but no phase control |
| `SmartLifecycle` | Long-running beans: subscribers, publishers, background workers |
| `@PreDestroy` | Simple sync cleanup; no async drain |

### Choosing between them

```
Does the bean run continuously in the background?
  └── YES → SmartLifecycle
  └── NO  → Does it need to run once at startup?
              └── YES → ApplicationRunner (after server ready)
                        or @PostConstruct (before server ready)
              └── NO  → @PostConstruct for init, @PreDestroy for cleanup
```

---

## 12. Common Pitfalls

### 12.1 Forgetting to call `callback.run()`

```java
// BUG: Spring hangs for 30s waiting for callback on shutdown
@Override
public void stop(Runnable callback) {
    running = false;
    try {
        executor.awaitTermination(10, SECONDS);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // callback never called → 30s hang
    }
}

// FIX: always call callback in finally
@Override
public void stop(Runnable callback) {
    running = false;
    try {
        executor.awaitTermination(10, SECONDS);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    } finally {
        callback.run();  // ← guaranteed
    }
}
```

### 12.2 Non-idempotent `start()`

```java
// BUG: calling start() twice opens two connections
@Override
public void start() {
    this.client = new HAClient();
    client.connect(uri);
    running = true;
}

// FIX: guard with AtomicBoolean CAS
@Override
public void start() {
    if (!running.compareAndSet(false, true)) return;
    this.client = new HAClient();
    client.connect(uri);
}
```

### 12.3 Blocking the caller thread in `stop(Runnable)`

```java
// BAD: blocks the Spring shutdown thread — delays context teardown
@Override
public void stop(Runnable callback) {
    semaphore.acquireUninterruptibly(maxConcurrency);  // may block for seconds
    callback.run();
}

// GOOD: drain on a separate thread so Spring can proceed with other phases
@Override
public void stop(Runnable callback) {
    Thread.ofVirtual().start(() -> {
        semaphore.acquireUninterruptibly(maxConcurrency);
        callback.run();
    });
}
```

### 12.4 Calling `@Transactional` methods from `@PostConstruct` via `this`

```java
// BUG: AOP proxy not created yet — @Transactional has no effect
@PostConstruct
public void init() {
    this.loadReferenceData();  // bypasses transaction interceptor
}

@Transactional
public void loadReferenceData() { ... }

// FIX A: inject self-proxy
@Autowired @Lazy private OrderService self;

@PostConstruct
public void init() {
    self.loadReferenceData();   // goes through proxy
}

// FIX B: move to ApplicationRunner / SmartLifecycle.start() — proxy guaranteed ready
```

### 12.5 Phase inversion — subscriber starts before DB is ready

```java
// If DataSource has no explicit phase it defaults to MAX_VALUE — same as subscriber!
// Fix: either give the DataSource a lower phase via a SmartLifecycle wrapper,
// or rely on @DependsOn to force creation order (different from lifecycle order):

@Component
@DependsOn("dataSource")    // ensures dataSource bean is created first (Stage 1-6)
public class MySubscriber implements SmartLifecycle { ... }
```

---

## 13. Testing SmartLifecycle Beans

```java
@SpringBootTest
class SingleAmpsSubscriberTest {

    @Autowired
    private SingleAmpsSubscriber subscriber;

    @Test
    void startsOnContextRefresh() {
        assertThat(subscriber.isRunning()).isTrue();
    }

    @Test
    void stopsGracefullyWithinTimeout() {
        assertThat(subscriber.isRunning()).isTrue();
        subscriber.stop();
        assertThat(subscriber.isRunning()).isFalse();
    }

    @Test
    void stopCallbackIsAlwaysCalled() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        subscriber.stop(latch::countDown);
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
```

---

## 14. Complete Spring Startup / Shutdown Timeline

```
JVM start
  │
  └── SpringApplication.run()
        │
        └── ApplicationContext created (empty shell)
              │
              └── context.refresh()
                    │
                    ├── Bean definitions read (no instances yet)
                    ├── BeanFactoryPostProcessors run   (@Value placeholders resolved)
                    ├── BeanPostProcessors registered
                    ├── Tomcat/Netty starts             (onRefresh hook)
                    ├── registerListeners()             (ApplicationListeners wired)
                    │
                    ├── finishBeanFactoryInitialization()
                    │     └── For every singleton bean:
                    │           Constructor → @Autowired → Aware → BPP-before →
                    │           @PostConstruct → BPP-after (AOP proxy)
                    │
                    └── finishRefresh()
                          ├── SmartLifecycle.start() — phase ordered (low→high)
                          │     Phase MIN_VALUE: Flyway / DB migrations
                          │     Phase -1000:     DataSource pool
                          │     Phase MAX_VALUE-1: Publisher
                          │     Phase MAX_VALUE:   Subscriber
                          │
                          └── ContextRefreshedEvent
                                └── @EventListener(ContextRefreshedEvent) fires
                                └── ApplicationReadyEvent (Spring Boot)
                                      └── ApplicationRunner.run()

Application running ...

JVM shutdown signal / context.close()
  │
  └── ContextClosedEvent
        └── @EventListener(ContextClosedEvent) fires
  │
  └── LifecycleProcessor.onClose()
        └── SmartLifecycle.stop(Runnable) — phase ordered (high→low)
              Phase MAX_VALUE:    Subscriber drains
              Phase MAX_VALUE-1:  Publisher drains
              Phase -1000:        DataSource pool closes
  │
  └── BeanFactory destroys singletons
        └── @PreDestroy methods run (synchronous)
        └── DisposableBean.destroy() methods run
  │
  └── JVM exits
```

---

## 15. Quick Reference

| Method | Default | Override when |
|---|---|---|
| `start()` | (abstract) | Open connections, start threads, register listeners |
| `stop(Runnable)` | calls `stop()` | Need async drain before signalling done |
| `stop()` | (abstract) | Simple sync cleanup path |
| `isRunning()` | (abstract) | Return your `AtomicBoolean` |
| `getPhase()` | `MAX_VALUE` | Need ordering relative to other lifecycle beans |
| `isAutoStartup()` | `true` | Conditional or lazy startup |

| Hook | Phase Control | Async Drain | AOP Safe | Context Ready |
|---|---|---|---|---|
| Constructor | No | No | No | No |
| `@PostConstruct` | No | No | No | No |
| `ApplicationRunner` | No | No | Yes | Yes |
| `@EventListener(ContextRefreshedEvent)` | No | No | Yes | Yes |
| `SmartLifecycle.start()` | Yes | Yes | Yes | Yes |
