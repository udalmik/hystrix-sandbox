package com.udalmik.hystrix.sandbox;

import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixInvokableInfo;
import com.netflix.hystrix.HystrixRequestLog;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import rx.Observable;
import rx.observables.BlockingObservable;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HystrixSandboxApplicationTests {

    @Test(timeout = 5000)
    @Ignore
    public void testNoThreadsBlockedOnNestedError_Failing() throws InterruptedException {

        testNoThreadsBlockedOnNestedErrorInternal(false);

    }

    private void testNoThreadsBlockedOnNestedErrorInternal(boolean useErrorHandlerForInnerSubscription) throws InterruptedException {
        final int num = 50;

        List<Callable<String>> tasksList = IntStream.range(0, num)
                .mapToObj(i -> (Callable<String>) () -> {
                    HystrixRequestContext context = HystrixRequestContext.initializeContext();
                    try {
                        Observable<String> observable = new CommandCollapserGetValueForKey(i).observe().flatMap(v -> {
                            Observable<String> errorObservable = Observable.error(new RuntimeException("Nested"));
                            if (useErrorHandlerForInnerSubscription) {
                                errorObservable.subscribe(x -> {
                                }, e -> {
                                });
                            } else {
                                errorObservable.subscribe(x -> {
                                });
                            }
                            return errorObservable;
                        });
                        return BlockingObservable.from(observable).first();
                    } finally {
                        context.shutdown();
                    }

                }).collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(num);
        List<Future<String>> futures = executor.invokeAll(tasksList);

        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
            }
        });
    }

    @Test(timeout = 5000)
    public void testNoThreadsBlockedOnNestedError() throws InterruptedException {

        testNoThreadsBlockedOnNestedErrorInternal(true);

    }

    @Test
    public void testCollapser() throws Exception {
        HystrixRequestContext context = HystrixRequestContext.initializeContext();
        try {
            Future<String> f1 = new CommandCollapserGetValueForKey(1).queue();
            Future<String> f2 = new CommandCollapserGetValueForKey(2).queue();
            Future<String> f3 = new CommandCollapserGetValueForKey(3).queue();
            Future<String> f4 = new CommandCollapserGetValueForKey(4).queue();

            assertEquals("ValueForKey: 1", f1.get());
            assertEquals("ValueForKey: 2", f2.get());
            assertEquals("ValueForKey: 3", f3.get());
            assertEquals("ValueForKey: 4", f4.get());

            int numExecuted = HystrixRequestLog.getCurrentRequest().getAllExecutedCommands().size();

            System.err.println("num executed: " + numExecuted);

            // assert that the batch command 'GetValueForKey' was in fact executed and that it executed only
            // once or twice (due to non-determinism of scheduler since this example uses the real timer)
            if (numExecuted > 2) {
                fail("some of the commands should have been collapsed");
            }

            System.err.println("HystrixRequestLog.getCurrentRequest().getAllExecutedCommands(): " + HystrixRequestLog.getCurrentRequest().getAllExecutedCommands());

            int numLogs = 0;
            for (HystrixInvokableInfo<?> command : HystrixRequestLog.getCurrentRequest().getAllExecutedCommands()) {
                numLogs++;

                // assert the command is the one we're expecting
                assertEquals("GetValueForKey", command.getCommandKey().name());

                System.err.println(command.getCommandKey().name() + " => command.getExecutionEvents(): " + command.getExecutionEvents());

                // confirm that it was a COLLAPSED command execution
                assertTrue(command.getExecutionEvents().contains(HystrixEventType.COLLAPSED));
                assertTrue(command.getExecutionEvents().contains(HystrixEventType.SUCCESS));
            }

            assertEquals(numExecuted, numLogs);
        } finally {
            context.shutdown();
        }
    }

}
