package com.redhat.gdemo;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNResult;
import org.kie.server.api.marshalling.MarshallingFormat;
import org.kie.server.api.model.ServiceResponse;
import org.kie.server.client.DMNServicesClient;
import org.kie.server.client.KieServicesClient;
import org.kie.server.client.KieServicesConfiguration;
import org.kie.server.client.KieServicesFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.redhat.gdemo.Utils.b;
import static com.redhat.gdemo.Utils.entry;
import static com.redhat.gdemo.Utils.mapOf;

public class TrafficViolationDemo {

    static Logger logger = LoggerFactory.getLogger(TrafficViolationDemo.class);

    protected DMNServicesClient dmnClient;
    public KieServicesConfiguration configuration;
    public static String containerId;
    public static String namespace;
    public static String name;
    public static AtomicInteger counter = new AtomicInteger(0);

    public static String SERVER_URL = "serverURL";
    public static String USERNAME = "username";
    public static String PASSWORD = "password";
    public static String CONTAINER_ID_PARAMETER_KEY = "containerId";

    protected void setupClients(KieServicesClient kieServicesClient) {
        this.dmnClient = kieServicesClient.getServicesClient(DMNServicesClient.class);
    }

    public TrafficViolationDemo() {
        String url = System.getProperty(SERVER_URL, "http://localhost:8090/rest/server");
        String username = System.getProperty(USERNAME, "wbadmin");
        String password = System.getProperty(PASSWORD, "wbadmin");

        configuration = KieServicesFactory.newRestConfiguration(url, username, password);
    }

    protected KieServicesClient createDefaultClient() throws Exception {

        configuration.setTimeout(3000);
        configuration.setMarshallingFormat(MarshallingFormat.JSON);

        KieServicesClient kieServicesClient = KieServicesFactory.newKieServicesClient(configuration);

        setupClients(kieServicesClient);
        return kieServicesClient;
    }

    public DMNServicesClient getDmnClient() {
        return dmnClient;
    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting kie-server requests");

        TrafficViolationDemo applicationClient = new TrafficViolationDemo();

        containerId = Optional.ofNullable(System.getProperty(CONTAINER_ID_PARAMETER_KEY))
                .orElseThrow(() -> new IllegalStateException("containerId parameter is mandatory"));

        namespace = "https://github.com/kiegroup/drools/kie-dmn/_A4BCA8B8-CF08-433F-93B2-A2598F19ECFF";
        name = "Traffic Violation";

        applicationClient.createDefaultClient();

        final int parallelism = args.length > 0 ? Integer.valueOf(args[0]) : 1;
        final ExecutorService executor = Executors.newFixedThreadPool(parallelism);
        final CyclicBarrier started = new CyclicBarrier(parallelism);
        final Callable<Long> task = () -> {
            started.await();
            final Thread current = Thread.currentThread();
            long executions = 0;
            while (!current.isInterrupted()) {
                evaluateDMNWithPause(applicationClient.getDmnClient());
                executions++;
                if (executions % 1000 == 0) {
                    logger.info(executions + " requests sent");
                }
            }
            logger.info("Thread '" + current.getId() + "' executed '" + executions + "'' requests.");
            return executions;
        };
        final ArrayList<Future<Long>> tasks = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            tasks.add(executor.submit(task));
        }
        executor.shutdown();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            tasks.forEach(future -> future.cancel(true));
        }));
    }

    private static void evaluateDMNWithPause(DMNServicesClient dmnClient) {
        try {
            DMNContext dmnContext = dmnClient.newContext();
            dmnContext.set("Driver", mapOf(entry("Points", b(15))));
            dmnContext.set("Violation", mapOf(entry("Type", "speed"),
                                              entry("Actual Speed", b(135)),
                                              entry("Speed Limit", b(100))));
            ServiceResponse<DMNResult> evaluateAll = dmnClient.evaluateAll(containerId, namespace, name, dmnContext);
            // logger.info("result" + evaluateAll.getMsg());
            Thread.sleep(500);
        } catch (org.kie.server.common.rest.NoEndpointFoundException nef) {
            logger.error("Error during DMN Execution. No endpoint found. Ignoring.", nef);
        } catch (Throwable t) {
            logger.error("Error during DMN Execution", t);
            throw new RuntimeException(t);
        }
    }
}
