package io.questdb.test.lifecycle;

import io.questdb.lifecycle.LifecycleContext;
import io.questdb.lifecycle.LifecycleOrchestrator;
import io.questdb.lifecycle.LifecycleSnapshot;
import io.questdb.lifecycle.LifecycleStartupException;
import io.questdb.lifecycle.Role;
import io.questdb.lifecycle.State;
import io.questdb.lifecycle.SwitchInFlightException;
import io.questdb.std.ObjList;
import io.questdb.test.lifecycle.fakes.BarrierComponent;
import io.questdb.test.lifecycle.fakes.CapturingLog;
import io.questdb.test.lifecycle.fakes.ProbeComponent;
import io.questdb.test.lifecycle.fakes.ThrowingComponent;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LifecycleOrchestratorTest {

    @Rule
    public Timeout timeout = Timeout.builder().withTimeout(30, TimeUnit.SECONDS).withLookingForStuckThread(true).build();

    @Test
    public void testEnvelopeExtraDepsInjection() {
        // B6 fix -- verify polymorphic dispatch through workerPoolManagerExtraHardDeps()-style hook.
        // We model it abstractly here: an "envelope" component whose hardDeps are concatenated from
        // a base list ["base"] + an extra list supplied by an overridden hook. The override is on
        // a subclass; the polymorphic call must invoke the subclass override even though construction
        // happens through the base path.
        class EnvelopeBase {
            ObjList<String> extraHardDeps() {
                return new ObjList<>();
            }
        }
        class EnvelopeSub extends EnvelopeBase {
            @Override
            ObjList<String> extraHardDeps() {
                ObjList<String> deps = new ObjList<>();
                deps.add("ent-pre-services");
                return deps;
            }
        }
        // The base envelope picks up its deps via the hook, polymorphically.
        EnvelopeBase sub = new EnvelopeSub();
        ObjList<String> deps = sub.extraHardDeps();
        Assert.assertEquals(1, deps.size());
        Assert.assertEquals("ent-pre-services", deps.getQuick(0));

        // Now wire a real orchestrator with a probe that uses sub.extraHardDeps()'s output as its hard deps.
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent base = new ProbeComponent("ent-pre-services");
        ProbeComponent envelope = new ProbeComponent("envelope", sub.extraHardDeps(), new ObjList<>());
        orch.register(base);
        orch.register(envelope);
        orch.run();
        // Envelope's start ran AFTER its injected hard dep reached READY.
        Assert.assertTrue("envelope must start after ent-pre-services",
                base.getStartSeq() < envelope.getStartSeq());
        Assert.assertEquals(State.READY, orch.stateOf("envelope"));
        orch.close();
    }

    @Test
    public void testFailedIsTerminal() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.publish(State.STARTING);
        ctx.publish(State.FAILED, "boom");
        ctx.publish(State.READY);
        Assert.assertEquals(State.FAILED, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testFailureCascadeHardDeps() {
        // WR-02: close() in finally so the orchestrator's ThreadPoolExecutor + SynchronousQueue
        // are released even on the validation-failure path.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ThrowingComponent a = new ThrowingComponent("a", () -> new RuntimeException("a-boom"));
            ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
            ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
            ProbeComponent d = new ProbeComponent("d", new ObjList<>(), listOf("c"));   // soft dep
            orch.register(a);
            orch.register(b);
            orch.register(c);
            orch.register(d);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException");
            } catch (LifecycleStartupException expected) {
                Assert.assertEquals(State.FAILED, orch.stateOf("a"));
                Assert.assertEquals(State.FAILED, orch.stateOf("b"));
                Assert.assertEquals(State.FAILED, orch.stateOf("c"));
                // Soft dependent NOT auto-cascaded -- observed events instead.
                Assert.assertNotEquals(State.FAILED, orch.stateOf("d"));
            }
        } finally {
            orch.close();
        }
    }

    @Test
    public void testInFlightStartNotInterrupted() throws Exception {
        // WR-06: the runner thread blocks inside BarrierComponent.start() until releaseBarrier()
        // is called. If the STARTING-state assertion below were to fail (or any unexpected throw),
        // the runner would leak until JUnit's 30s @Rule Timeout fires. Wrap in try/finally so
        // releaseBarrier() + runner.join() + orch.close() always run.
        // WR-07: replace Thread.sleep(100) with BarrierComponent.awaitEntered(). The entered latch
        // flips before start()'s blocking await, so a successful awaitEntered() guarantees the
        // runner has actually reached the barrier and the orchestrator has published STARTING.
        LifecycleOrchestrator orch = newOrchestrator();
        BarrierComponent x = new BarrierComponent("x");
        orch.register(x);
        Thread runner = new Thread(() -> {
            try {
                orch.run();
            } catch (LifecycleStartupException ignore) {
            }
        });
        runner.start();
        try {
            Assert.assertTrue("BarrierComponent.start() did not enter within 10s -- runner did not schedule",
                    x.awaitEntered(10_000L));
            Assert.assertEquals(State.STARTING, orch.stateOf("x"));
        } finally {
            x.releaseBarrier();
            runner.join();
            orch.close();
        }
        Assert.assertTrue(x.isStarted());
    }

    @Test
    public void testParallelStartsIndependentComponents() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("a"), new ObjList<>());
        ProbeComponent d = new ProbeComponent("d", listOf("b", "c"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.register(d);
        orch.run();
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        Assert.assertEquals(State.READY, orch.stateOf("b"));
        Assert.assertEquals(State.READY, orch.stateOf("c"));
        Assert.assertEquals(State.READY, orch.stateOf("d"));
        orch.close();
    }

    @Test
    public void testProgressLatestOverwrites() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.progress(new TestProgressEvent("first"));
        ctx.progress(new TestProgressEvent("second"));
        LifecycleSnapshot snap = orch.snapshot();
        LifecycleSnapshot.ComponentSnapshot cs = snap.components().getQuick(0);
        Assert.assertNotNull(cs.latestProgress());
        Assert.assertTrue(cs.latestProgress() instanceof TestProgressEvent);
        Assert.assertEquals("second", ((TestProgressEvent) cs.latestProgress()).tag());
        orch.close();
    }

    @Test
    public void testPropagatesDependencyState() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.run();
        boolean sawAReady = false;
        for (ProbeComponent.Event e : b.events) {
            if ("a".equals(e.depName) && e.current == State.READY) sawAReady = true;
        }
        Assert.assertTrue("b did not observe a -> READY transition", sawAReady);
        orch.close();
    }

    @Test
    public void testRejectsDuplicateName() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a1 = new ProbeComponent("a");
        ProbeComponent a2 = new ProbeComponent("a");
        orch.register(a1);
        try {
            orch.register(a2);
            Assert.fail("expected LifecycleStartupException for duplicate name");
        } catch (LifecycleStartupException expected) {
            TestUtils.assertContains(expected.getMessage(), "duplicate component name");
        }
        orch.close();
    }

    @Test
    public void testRejectsUnknownDependency() {
        // WR-02: close() in finally so the orchestrator's ThreadPoolExecutor + SynchronousQueue
        // are released even on the validation-failure path.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a", listOf("nonexistent"), new ObjList<>());
            orch.register(a);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException for unknown dep");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "unknown dependency");
            }
        } finally {
            orch.close();
        }
    }

    @Test
    public void testRunIsRetryableAfterValidationFailure() {
        // WR-05: a validation failure (unknown dep) must NOT lock the orchestrator into a
        // permanent "run may only be called once" state. After fixing the registry, run()
        // should proceed normally on the retry.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent broken = new ProbeComponent("broken", listOf("nonexistent"), new ObjList<>());
            orch.register(broken);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException for unknown dep");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "unknown dependency");
            }
            // Register the missing dep and retry.
            orch.register(new ProbeComponent("nonexistent"));
            orch.run();   // must NOT throw IllegalStateException("may only be called once")
            Assert.assertEquals(State.READY, orch.stateOf("broken"));
            Assert.assertEquals(State.READY, orch.stateOf("nonexistent"));
        } finally {
            orch.close();
        }
    }

    @Test
    public void testReverseTopologicalShutdown() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.run();
        orch.close();
        Assert.assertTrue(c.getStopSeq() < b.getStopSeq());
        Assert.assertTrue(b.getStopSeq() < a.getStopSeq());
    }

    @Test
    public void testSnapshotEventuallyConsistent() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b");
        orch.register(a);
        orch.register(b);
        orch.run();
        LifecycleSnapshot snap = orch.snapshot();
        Assert.assertEquals(2, snap.components().size());
        Assert.assertTrue(snap.capturedAtMicros() > 0);
        for (int i = 0, n = snap.components().size(); i < n; i++) {
            LifecycleSnapshot.ComponentSnapshot cs = snap.components().getQuick(i);
            Assert.assertEquals(State.READY, cs.state());
            Assert.assertTrue(cs.lastTransitionMicros() > 0);
        }
        orch.close();
    }

    @Test
    public void testStartsInTopologicalOrder() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
        ProbeComponent d = new ProbeComponent("d", listOf("c"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.register(d);
        orch.run();
        Assert.assertTrue("a must start before b", a.getStartSeq() < b.getStartSeq());
        Assert.assertTrue("b must start before c", b.getStartSeq() < c.getStartSeq());
        Assert.assertTrue("c must start before d", c.getStartSeq() < d.getStartSeq());
        orch.close();
    }

    @Test
    public void testStoppedOnlyFromStopping() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent p = new ProbeComponent("a");
        orch.register(p);
        LifecycleContext ctx = orch.contextFor("a");
        ctx.publish(State.STARTING);
        ctx.publish(State.READY);
        ctx.publish(State.STOPPED);   // direct READY -> STOPPED rejected
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testSubmitSwitchCASSerialization() throws Exception {
        // D6-04: a second submitSwitch while the first is in flight throws SwitchInFlightException
        // with currentRole + targetRole reflecting the live orchestrator state.
        LifecycleOrchestrator orch = newOrchestrator();
        CountDownLatch firstSwitchEntered = new CountDownLatch(1);
        CountDownLatch firstSwitchRelease = new CountDownLatch(1);
        ProbeComponent blocking = new ProbeComponent("a") {
            @Override
            public void switchRole(LifecycleContext ctx, Role newRole) {
                firstSwitchEntered.countDown();
                try {
                    firstSwitchRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        orch.register(blocking);
        orch.run();
        try {
            orch.submitSwitch(Role.REPLICA);
            Assert.assertTrue("first switch did not enter within 5s", firstSwitchEntered.await(5, TimeUnit.SECONDS));
            try {
                orch.submitSwitch(Role.PRIMARY);
                Assert.fail("expected SwitchInFlightException");
            } catch (SwitchInFlightException expected) {
                // WR-03: role.set(newRole) now runs AFTER the cascade completes, so the
                // orchestrator's currentRole during an in-flight switch is the OLD role.
                // The first submitSwitch(REPLICA) entered switchRole but is blocked before the
                // cascade returns, so role.get() still reports the boot-time PRIMARY.
                Assert.assertEquals(Role.PRIMARY, expected.currentRole());
                Assert.assertEquals(Role.PRIMARY, expected.targetRole());
            }
            firstSwitchRelease.countDown();
            // Wait for the in-flight flag to clear.
            long deadline = System.currentTimeMillis() + 5_000L;
            while (orch.isSwitchInFlight() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertFalse("switchInFlight did not clear after first switch", orch.isSwitchInFlight());
            // A third submitSwitch succeeds.
            orch.submitSwitch(Role.PRIMARY);
            deadline = System.currentTimeMillis() + 5_000L;
            while (orch.isSwitchInFlight() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertFalse(orch.isSwitchInFlight());
            Assert.assertEquals(Role.PRIMARY, orch.currentRole());
        } finally {
            // Ensure the blocking switch is released so close() can shut down the executor cleanly.
            firstSwitchRelease.countDown();
            orch.close();
        }
    }

    @Test
    public void testSubmitSwitchClearsFlagOnFailure() throws Exception {
        // D6-06: when switchRole throws, the executor's UncaughtExceptionHandler fires
        // System.exit(55) via exitOnSwitchFailure(). The finally block in submitSwitch's lambda
        // clears switchInFlight BEFORE the exception escapes to the UEH. Use the package-private
        // exitOnSwitchFailure() seam to capture the exit code without terminating the JVM.
        AtomicInteger capturedExit = new AtomicInteger(-1);
        CountDownLatch exitCalled = new CountDownLatch(1);
        LifecycleOrchestrator orch = new LifecycleOrchestrator(Role.PRIMARY, null, null, null) {
            @Override
            protected void closeLogAndExit(int code) {
                capturedExit.set(code);
                exitCalled.countDown();
            }
        };
        try {
            ProbeComponent throwingOnSwitch = new ProbeComponent("a") {
                @Override
                public void switchRole(LifecycleContext ctx, Role newRole) {
                    throw new RuntimeException("simulated switch failure");
                }
            };
            orch.register(throwingOnSwitch);
            orch.run();
            orch.submitSwitch(Role.REPLICA);
            // Wait for the UEH to fire.
            Assert.assertTrue("exit hook did not fire within 5s", exitCalled.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(55, capturedExit.get());
            Assert.assertFalse("switchInFlight must clear before exit hook fires", orch.isSwitchInFlight());
        } finally {
            // close() was already invoked by the UEH; call again is a no-op (closed CAS guard).
            orch.close();
        }
    }

    @Test
    public void testSubmitSwitchClearsFlagOnSuccess() throws Exception {
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a");
            orch.register(a);
            orch.run();
            orch.submitSwitch(Role.REPLICA);
            long deadline = System.currentTimeMillis() + 5_000L;
            while (orch.isSwitchInFlight() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertFalse(orch.isSwitchInFlight());
            Assert.assertEquals(Role.REPLICA, orch.currentRole());
            orch.submitSwitch(Role.PRIMARY);
            deadline = System.currentTimeMillis() + 5_000L;
            while (orch.isSwitchInFlight() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertFalse(orch.isSwitchInFlight());
            Assert.assertEquals(Role.PRIMARY, orch.currentRole());
        } finally {
            orch.close();
        }
    }

    @Test
    public void testSubmitSwitchExecutorRunsTask() throws Exception {
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            AtomicReference<String> threadName = new AtomicReference<>();
            CountDownLatch ran = new CountDownLatch(1);
            ProbeComponent capture = new ProbeComponent("a") {
                @Override
                public void switchRole(LifecycleContext ctx, Role newRole) {
                    threadName.set(Thread.currentThread().getName());
                    ran.countDown();
                }
            };
            orch.register(capture);
            orch.run();
            orch.submitSwitch(Role.REPLICA);
            Assert.assertTrue("switch task did not run within 5s", ran.await(5, TimeUnit.SECONDS));
            String name = threadName.get();
            Assert.assertNotNull(name);
            Assert.assertTrue("expected lifecycle-N thread name, got " + name, name.matches("lifecycle-\\d+"));
        } finally {
            orch.close();
        }
    }

    @Test
    public void testSwitchRoleCascadeFailedThroughHardDeps() {
        // D6-06: on switchRole failure, FAILED cascades to hard-dependents BEFORE the exception
        // propagates. The throw still happens; the cascade is a side effect.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a");
            ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>()) {
                @Override
                public void switchRole(LifecycleContext ctx, Role newRole) {
                    throw new RuntimeException("b-switch-boom");
                }
            };
            ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
            orch.register(a);
            orch.register(b);
            orch.register(c);
            orch.run();
            try {
                orch.switchRole(Role.REPLICA);
                Assert.fail("expected LifecycleStartupException");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "switch failed at component b");
            }
            Assert.assertEquals(State.FAILED, orch.stateOf("b"));
            // c hard-deps on b -- cascade marked it FAILED before the throw propagated.
            Assert.assertEquals(State.FAILED, orch.stateOf("c"));
        } finally {
            orch.close();
        }
    }

    @Test
    public void testSwitchRoleDefaultStopStart() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        orch.register(a);
        orch.run();
        long startBefore = a.getStartSeq();
        orch.switchRole(Role.REPLICA);
        // Default switchRole = stop(); start(ctx).
        Assert.assertTrue("stop seq must be after initial start", a.getStopSeq() > startBefore);
        // B5 fix: orchestrator auto-publishes READY after dispatch.
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        orch.close();
    }

    @Test
    public void testSwitchRoleDispatchOrder() {
        LifecycleOrchestrator orch = newOrchestrator();
        ProbeComponent a = new ProbeComponent("a");
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        orch.register(c);
        orch.run();
        orch.switchRole(Role.REPLICA);
        // Dispatch order: a's stop < b's stop < c's stop (topo order).
        Assert.assertTrue(a.getStopSeq() < b.getStopSeq());
        Assert.assertTrue(b.getStopSeq() < c.getStopSeq());
        // B5 fix: orchestrator auto-publishes READY after each component's switchRole returns,
        // mirroring the auto-publish-after-start pattern.
        Assert.assertEquals(State.READY, orch.stateOf("a"));
        Assert.assertEquals(State.READY, orch.stateOf("b"));
        Assert.assertEquals(State.READY, orch.stateOf("c"));
        orch.close();
    }

    @Test
    public void testSwitchRoleFailFastThrowsOnFirstThrow() {
        // D6-06: on the first throw from c.switchRole, orchestrator publishes FAILED on the
        // offending component, cascades through hard-deps, throws LifecycleStartupException.
        // Subsequent components in topoOrder are NOT invoked.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a");
            AtomicBoolean bSwitchInvoked = new AtomicBoolean(false);
            ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>()) {
                @Override
                public void switchRole(LifecycleContext ctx, Role newRole) {
                    bSwitchInvoked.set(true);
                    throw new RuntimeException("b-switch-boom");
                }
            };
            AtomicBoolean cSwitchInvoked = new AtomicBoolean(false);
            ProbeComponent c = new ProbeComponent("c", listOf("b"), new ObjList<>()) {
                @Override
                public void switchRole(LifecycleContext ctx, Role newRole) {
                    cSwitchInvoked.set(true);
                    super.switchRole(ctx, newRole);
                }
            };
            orch.register(a);
            orch.register(b);
            orch.register(c);
            orch.run();
            try {
                orch.switchRole(Role.REPLICA);
                Assert.fail("expected LifecycleStartupException");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "switch failed at component b");
            }
            Assert.assertTrue("b.switchRole must have been invoked", bSwitchInvoked.get());
            Assert.assertFalse("c.switchRole must NOT have been invoked (fail-fast)", cSwitchInvoked.get());
            Assert.assertEquals(State.FAILED, orch.stateOf("b"));
            // c was cascaded to FAILED via hard-dep BFS, not via its own switchRole.
            Assert.assertEquals(State.FAILED, orch.stateOf("c"));
        } finally {
            orch.close();
        }
    }

    @Test
    public void testTransitionLogShape() {
        CapturingLog capture = new CapturingLog();
        LifecycleOrchestrator orch = new LifecycleOrchestrator(Role.PRIMARY, capture, null, null);
        ProbeComponent a = new ProbeComponent("a");
        orch.register(a);
        orch.run();
        String text = capture.sink.toString();
        TestUtils.assertContains(text, "component=a");
        TestUtils.assertContains(text, " from=INIT");
        TestUtils.assertContains(text, " to=STARTING");
        TestUtils.assertContains(text, " ts=");
        TestUtils.assertContains(text, " since=");
        // FAILED transition includes reason.
        capture.sink.clear();
        LifecycleOrchestrator orch2 = new LifecycleOrchestrator(Role.PRIMARY, capture, null, null);
        ThrowingComponent t = new ThrowingComponent("t", () -> new RuntimeException("kaboom"));
        orch2.register(t);
        try {
            orch2.run();
        } catch (LifecycleStartupException ignore) {
        }
        TestUtils.assertContains(capture.sink.toString(), "to=FAILED");
        TestUtils.assertContains(capture.sink.toString(), "reason=\"");
        orch.close();
        orch2.close();
    }

    @Test
    public void testValidateBeforeRunningFlag() {
        // B3 fix -- orchestrator validates the DAG BEFORE flipping running=true.
        // A cycle in the registered components must throw LifecycleStartupException
        // from run(). After that throw, register() is closed off (single-shot lifecycle),
        // and close() must run cleanly without NPE on reverseTopoOrder.
        LifecycleOrchestrator orch = new LifecycleOrchestrator(Role.PRIMARY, null, null, null);
        ProbeComponent a = new ProbeComponent("a", listOf("b"), new ObjList<>());
        ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
        orch.register(a);
        orch.register(b);
        try {
            orch.run();
            Assert.fail("expected LifecycleStartupException for cycle");
        } catch (LifecycleStartupException expected) {
            TestUtils.assertContains(expected.getMessage(), "dependency cycle detected");
        }
        // No component was ever asked to start.
        Assert.assertEquals(-1, a.getStartSeq());
        Assert.assertEquals(-1, b.getStartSeq());
        // close() must be defensive against null reverseTopoOrder -- must NOT throw NPE.
        orch.close();

        // A fresh orchestrator with a valid DAG works after the failed one was closed.
        LifecycleOrchestrator fresh = new LifecycleOrchestrator(Role.PRIMARY, null, null, null);
        ProbeComponent c = new ProbeComponent("c");
        fresh.register(c);
        fresh.run();
        Assert.assertEquals(State.READY, fresh.stateOf("c"));
        fresh.close();
    }

    @Test
    public void testValidatesDagAcyclic() {
        // WR-02: close() in finally so the orchestrator's ThreadPoolExecutor + SynchronousQueue
        // are released even on the validation-failure path.
        LifecycleOrchestrator orch = newOrchestrator();
        try {
            ProbeComponent a = new ProbeComponent("a", listOf("b"), new ObjList<>());
            ProbeComponent b = new ProbeComponent("b", listOf("a"), new ObjList<>());
            orch.register(a);
            orch.register(b);
            try {
                orch.run();
                Assert.fail("expected LifecycleStartupException for cycle");
            } catch (LifecycleStartupException expected) {
                TestUtils.assertContains(expected.getMessage(), "dependency cycle detected");
            }
        } finally {
            orch.close();
        }
    }

    @Test
    public void testWpmTwoPhaseDegradedToReady() {
        // Simulate WPM two-phase: register a "wpm" probe + a downstream "service".
        // wpm.start(ctx) publishes DEGRADED then registers onStableBelow("wpm", ...) callback
        // that publishes READY when the downstream is stable.
        LifecycleOrchestrator orch = newOrchestrator();
        AtomicReference<LifecycleContext> wpmCtxRef = new AtomicReference<>();
        ProbeComponent wpm = new ProbeComponent("wpm") {
            @Override
            public void start(LifecycleContext ctx) {
                super.start(ctx);
                wpmCtxRef.set(ctx);
                ctx.publish(State.DEGRADED);
                ctx.onStableBelow("wpm", () -> ctx.publish(State.READY));
            }
        };
        ProbeComponent service = new ProbeComponent("service", listOf("wpm"), new ObjList<>());
        orch.register(wpm);
        orch.register(service);
        orch.run();
        Assert.assertEquals(State.READY, orch.stateOf("wpm"));
        Assert.assertEquals(State.READY, orch.stateOf("service"));
        orch.close();
    }

    private static ObjList<String> listOf(String... names) {
        ObjList<String> l = new ObjList<>();
        for (String n : names) l.add(n);
        return l;
    }

    private static LifecycleOrchestrator newOrchestrator() {
        return new LifecycleOrchestrator(Role.PRIMARY, null, null, null);
    }
}
