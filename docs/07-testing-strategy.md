# 07 — Testing Strategy

## Philosophy

- **Unit tests**: fast, isolated, no Spring context, no DB, no AMPS. Use Mockito.
- **Integration tests**: full Spring context with a real (in-memory) DB and a mock AMPS client.
  Use `@SpringBootTest` with `@ActiveProfiles("test")`.
- **No real AMPS server required** for any automated test. AMPS SDK classes are mocked or
  replaced with test doubles.

---

## Test Directory Layout

```text
src/test/java/.../
│
├── unit/
│   ├── processor/
│   │   └── MessageProcessorTest.java         (Mockito, no Spring)
│   ├── service/
│   │   └── MessageDispatchServiceTest.java   (Mockito, no Spring)
│   ├── subscriber/
│   │   ├── SingleAmpsSubscriberTest.java     (Mockito, no Spring)
│   │   └── MultiAmpsSubscriberPoolTest.java  (Mockito, no Spring)
│   └── health/
│       └── AmpsHealthIndicatorTest.java      (Mockito, no Spring)
│
└── integration/
    ├── SingleSubscriberIntegrationTest.java  (@SpringBootTest, profile=test-single)
    ├── MultiSubscriberIntegrationTest.java   (@SpringBootTest, profile=test-multi)
    └── IdempotencyIntegrationTest.java       (@SpringBootTest, duplicate delivery test)

src/test/resources/
├── application-test-single.yaml             H2 + mock AMPS, single-subscriber profile
├── application-test-multi.yaml              H2 + mock AMPS, multi-subscriber profile
└── logback-test.xml                         quieter logging during tests
```

---

## Test Profiles

```yaml
# application-test-single.yaml
spring:
  profiles:
    active: single-subscriber,test-single

  datasource:
    url: jdbc:h2:mem:testdb-single;DB_CLOSE_DELAY=-1
    driver-class-name: org.h2.Driver
    username: sa
    password: ""
    hikari:
      maximum-pool-size: 5

  jpa:
    hibernate:
      ddl-auto: create-drop
    show-sql: true

amps:
  server:
    uri: tcp://localhost:19004/amps/json    # unused — HAClient is mocked
  client:
    name: test-subscriber
  queue:
    topic: /queue/test-trades
  consumer:
    max-concurrency: 10
    max-retries: 3
```

```yaml
# application-test-multi.yaml
spring:
  profiles:
    active: multi-subscriber,test-multi

  datasource:
    url: jdbc:h2:mem:testdb-multi;DB_CLOSE_DELAY=-1
    # ...same as above...

amps:
  consumer:
    subscriber-count: 2
    max-concurrency-per-subscriber: 5
    max-retries: 3
```

---

## Mock AMPS Client — `MockHaClient`

Because the real `HAClient` requires a running AMPS server, integration tests use a
`MockHaClient` that implements the same interface used in tests.

```text
MockHaClient responsibilities:
  - Holds a BlockingQueue<Message> as the "queue"
  - bookmarkSubscribe(topic) → returns a MockMessageStream that drains the queue
  - ack(message)            → records the ACK (list of ack'd bookmarks)
  - close()                 → no-op or signals MockMessageStream to stop iterating
  - getConnectionState()    → returns CONNECTED always (for health check tests)

MockMessageStream:
  - Implements Iterable<Message>
  - iterator.next() → queue.poll(500ms, MILLISECONDS) → null → StopIteration
  - Allows tests to inject messages and observe ACKs
```

In Spring test config, replace the real `HAClient` bean:

```java
// In a @TestConfiguration class:
@Profile("test-single")
@Bean(name = "testHaClient")
@Primary
public HAClient mockHaClient() {
    return new MockHaClient();  // or Mockito.mock(HAClient.class) + stub
}
```

---

## Unit Tests

### `MessageProcessorTest`

Tests the processor in isolation. No Spring, no DB — uses a mocked `ProcessedMessageRepository`.

```text
Test cases:

  test_newMessage_success():
    given: repo.findByMessageId() → empty
    given: business logic succeeds
    when:  processor.process(msg)
    then:  repo.save() called with status=PROCESSED, retryCount=0
    then:  result == OK

  test_duplicate_processedRecord():
    given: repo.findByMessageId() → ProcessedMessage(status=PROCESSED)
    when:  processor.process(msg)
    then:  repo.save() NOT called
    then:  result == DUPLICATE

  test_duplicateInsert_dataIntegrityViolation():
    given: repo.findByMessageId() → empty (race condition — record inserted between check and save)
    given: repo.save() throws DataIntegrityViolationException
    when:  processor.process(msg)
    then:  result == DUPLICATE  (treated same as idempotency check)

  test_firstFailure_setsFailedStatus():
    given: repo.findByMessageId() → empty
    given: business logic throws RuntimeException
    when:  processor.process(msg)
    then:  repo.save() called with status=FAILED, retryCount=1
    then:  exception rethrown (so caller knows to no-ACK)

  test_retry_incrementsRetryCount():
    given: repo.findByMessageId() → ProcessedMessage(status=FAILED, retryCount=1)
    given: maxRetries = 3
    given: business logic throws RuntimeException
    when:  processor.process(msg)
    then:  repo.save() called with retryCount=2

  test_maxRetriesExceeded_returnsDiscard():
    given: repo.findByMessageId() → ProcessedMessage(status=FAILED, retryCount=3)
    given: maxRetries = 3
    when:  processor.process(msg)
    then:  repo.save() called with status=DISCARDED
    then:  result == DISCARD (no exception thrown — caller will ACK)

  test_alreadyDiscarded_returnsDiscard():
    given: repo.findByMessageId() → ProcessedMessage(status=DISCARDED)
    when:  processor.process(msg)
    then:  repo.save() NOT called
    then:  result == DISCARD
```

---

### `MessageDispatchServiceTest`

```text
Test cases:

  test_dispatch_acquiresSemaphoreBeforeSubmit():
    given: semaphore with 1 permit
    given: processor succeeds
    when:  dispatch(msg, mockClient)
    then:  semaphore.availablePermits() briefly = 0 (during VT execution)
    then:  semaphore.availablePermits() = 1 (after VT finishes)

  test_dispatch_acks_onSuccess():
    given: processor returns OK
    when:  dispatch(msg, mockClient)
    then:  mockClient.ack(msg) called once

  test_dispatch_noAck_onFailure():
    given: processor throws RuntimeException
    when:  dispatch(msg, mockClient)
    then:  mockClient.ack(msg) NOT called
    then:  semaphore.availablePermits() = 1 (released in finally)

  test_dispatch_acks_onDuplicate():
    given: processor returns DUPLICATE
    when:  dispatch(msg, mockClient)
    then:  mockClient.ack(msg) called once

  test_dispatch_acks_onDiscard():
    given: processor returns DISCARD
    when:  dispatch(msg, mockClient)
    then:  mockClient.ack(msg) called once

  test_semaphoreBlocks_whenFull():
    given: semaphore with 2 permits
    given: 3 messages dispatched simultaneously
    then:  first 2 dispatched immediately
    then:  3rd blocks until a VT releases
```

---

### `SingleAmpsSubscriberTest`

```text
Test cases:

  test_start_spawnsOnePlatformThread():
    when:  subscriber.start()
    then:  subscriber.isRunning() == true
    then:  thread named "amps-subscriber" exists

  test_stop_setsRunningFalse():
    given: subscriber.start() called
    when:  subscriber.stop()
    then:  subscriber.isRunning() == false

  test_receiveLoop_dispatches_eachMessage():
    given: mock stream yields [m1, m2, m3] then stops
    when:  subscriber.start()
    then:  dispatchService.dispatch() called 3 times

  test_receiveLoop_stops_onRunningFalse():
    given: mock stream yields messages indefinitely
    given: running set to false after 5 messages
    then:  dispatch called exactly 5 times
```

---

### `AmpsHealthIndicatorTest`

```text
Test cases:

  test_healthUp_whenAllConnected():
    given: all HAClients return CONNECTED state
    when:  health()
    then:  Health.up() with details for each client

  test_healthDown_whenAnyDisconnected():
    given: one HAClient returns DISCONNECTED
    when:  health()
    then:  Health.down() with the disconnected client flagged

  test_healthDown_whenClientThrows():
    given: HAClient.getConnectionState() throws exception
    when:  health()
    then:  Health.down() with exception message in details
```

---

## Integration Tests

### `SingleSubscriberIntegrationTest`

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"single-subscriber", "test-single"})
class SingleSubscriberIntegrationTest {
```

```text
Test cases:

  test_endToEnd_singleMessage():
    given: MockHaClient.enqueue(buildMessage("msg-001", payload))
    when:  application context starts (subscriber auto-starts)
    then:  ProcessedMessage with messageId="msg-001" exists in DB with status=PROCESSED
    then:  MockHaClient.isAcked("msg-001") == true

  test_endToEnd_concurrentMessages(100):
    given: MockHaClient.enqueue(100 messages with unique bookmarks)
    when:  all 100 processed
    then:  ProcessedMessageRepository.count() == 100
    then:  all 100 have status=PROCESSED
    then:  no exceptions thrown

  test_idempotency_duplicateDelivery():
    given: same message (bookmark = "dup-001") enqueued twice
    when:  both delivered (simulate via MockHaClient)
    then:  ProcessedMessageRepository.count() where messageId="dup-001" == 1
    then:  both deliveries ACK'd (no infinite re-delivery)

  test_retryBehavior_failThenSucceed():
    given: business logic configured to fail for first 2 attempts then succeed
    given: message "retry-001" enqueued 3 times (simulating AMPS re-delivery)
    when:  processed
    then:  final status = PROCESSED, retryCount = 2

  test_maxRetries_discardsMessage():
    given: business logic always throws for message "bad-001"
    given: maxRetries = 3
    given: message enqueued 4 times (3 retries + 1 final)
    when:  all 4 deliveries processed
    then:  status = DISCARDED, retryCount = 3
    then:  all 4 deliveries ACK'd (no more re-delivery)
```

---

### `IdempotencyIntegrationTest`

```text
test_concurrentDuplicateDelivery():
  given: same message delivered simultaneously to 3 subscriber threads
  when:  all 3 threads try to INSERT concurrently
  then:  exactly 1 row in DB
  then:  all 3 VTs ACK the message (no unhandled exception)
  then:  amps.messages.duplicate counter == 2

  (This test verifies that the DataIntegrityViolationException catch path
   is thread-safe and does not leave any subscriber thread in a bad state)
```

---

## Test Utilities

### `TestMessageBuilder`

```text
static Message buildMessage(String bookmarkId, String payload):
  Creates a Mockito mock of com.crankuptheamps.client.Message
  Stubs:
    msg.getBookmark() → bookmarkId
    msg.getData()     → payload
    msg.getTopic()    → "/queue/test-trades"
    msg.getHeader(*)  → ""
```

### `TestAwaitility`

```text
Use Awaitility for asynchronous assertions:

  await().atMost(5, SECONDS)
         .until(() → repo.existsByMessageId("msg-001"));

This avoids Thread.sleep() and makes tests resilient to VT scheduling variance.
```

---

## Test Coverage Targets

| Layer | Target Coverage | Priority |
|---|---|---|
| `MessageProcessor` | 90%+ | Critical — all retry/idempotency paths |
| `MessageDispatchService` | 85%+ | Critical — backpressure, ACK/no-ACK |
| `SingleAmpsSubscriber` | 80%+ | Important — lifecycle correctness |
| `MultiAmpsSubscriberPool` | 80%+ | Important — multi-thread lifecycle |
| `AmpsHealthIndicator` | 75%+ | Standard |
| Config classes | Not unit-tested | Covered by integration tests |
| Repository | Not unit-tested | Spring Data — covered by integration |

---

## CI Pipeline Recommendations

```text
Stage 1: Unit tests
  ./mvnw test -Dgroups=unit
  Target: < 30 seconds
  No external dependencies

Stage 2: Integration tests (single-subscriber)
  ./mvnw test -Dgroups=integration -Dspring.profiles.active=test-single
  Target: < 2 minutes
  Uses H2 in-memory DB

Stage 3: Integration tests (multi-subscriber)
  ./mvnw test -Dgroups=integration -Dspring.profiles.active=test-multi
  Target: < 2 minutes

Optional Stage 4: PostgreSQL integration (Testcontainers)
  Uses org.testcontainers:postgresql
  Spins up a real PostgreSQL container for multi-JVM idempotency tests
  Target: < 5 minutes
```
