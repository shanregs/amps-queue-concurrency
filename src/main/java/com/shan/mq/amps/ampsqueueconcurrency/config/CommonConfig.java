package com.shan.mq.amps.ampsqueueconcurrency.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Shared beans loaded unconditionally across ALL Spring profiles.
 *
 * <p>The virtual-thread executor is profile-agnostic because every active profile
 * (single-subscriber, multi-subscriber, multi-jvm-subscriber, message-publisher)
 * dispatches concurrent work to virtual threads.
 *
 * <h3>Why virtual threads fit this workload</h3>
 * <pre>
 *   Platform thread:  ~1 MB stack, OS-scheduled, max ~10 K before OOM
 *   Virtual thread:   ~1-2 KB heap, JVM-scheduled, millions possible
 *
 *   Blocking I/O on a platform thread → OS thread held, carrier unavailable
 *   Blocking I/O on a virtual thread  → VT parks (continuation on heap),
 *                                        carrier thread freed immediately
 * </pre>
 * Result: 20 HikariCP connections comfortably serve 200+ in-flight VTs.
 */
@Slf4j
@Configuration
public class CommonConfig {

    /**
     * Unbounded virtual-thread-per-task executor.
     *
     * <p>Each call to {@code executor.submit(task)} launches exactly one virtual
     * thread. The JDK's ForkJoinPool schedules these over {@code N} carrier threads
     * (one per CPU core by default).
     *
     * <p>Threads are named {@code "amps-vt-0"}, {@code "amps-vt-1"}, … so they
     * stand out immediately in thread-dumps and profiler flame graphs.
     *
     * <p>Spring calls {@code shutdown()} on the bean during context close, allowing
     * already-submitted virtual threads to complete before the JVM exits.
     */
    @Bean(name = "ampsVirtualThreadExecutor", destroyMethod = "shutdown")
    public ExecutorService ampsVirtualThreadExecutor() {
        ThreadFactory factory = Thread.ofVirtual()
                .name("amps-vt-", 0)        // names: amps-vt-0, amps-vt-1, ...
                .factory();
        ExecutorService executor = Executors.newThreadPerTaskExecutor(factory);
        log.info("AMPS virtual-thread executor ready (newThreadPerTaskExecutor, prefix=amps-vt-)");
        return executor;
    }
}
