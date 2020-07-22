package es.predictia.rserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Ignore("necesita R con paquete RServer instalado en local")
@Slf4j
public class RSessionFactoryTest {
	
	@Test
	public void testDefaultSessionsRequest() throws Exception{
		RSessionFactory rSessionFactory = defaultRSessionFactory();
		var worker1 = new SimpleRWorker(session -> session.eval("sessionInfo()"));
		var worker2 = new SimpleRWorker(session -> session.eval("sessionInfo()"));
		RWorkers.runWithinRsession(worker1, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.runWithinRsession(worker2, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.wait(60, TimeUnit.SECONDS, worker1, worker2);
	}
	
	private static RSessionFactory defaultRSessionFactory(){
		RSessionFactory rSessionFactory = new RSessionFactory();
		return rSessionFactory;
	}
	
	@Test(expected=InterruptedException.class)
	public void testUnfinishedDefaultSessionRequest() throws Exception{
		RSessionFactory rSessionFactory = defaultRSessionFactory();
		var worker1 = new SimpleRWorker(session -> session.eval("sessionInfo()"));
		var worker2 = new SimpleRWorker(session -> session.eval("sessionInfo()"));
		var waitWorker = waitWorker(10, TimeUnit.MINUTES);
		RWorkers.runWithinRsession(worker1, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.runWithinRsession(worker2, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.wait(15, TimeUnit.SECONDS, worker1, worker2, waitWorker);
	}

	@Test
	public void testExpiringSessionRequest() throws Exception{
		RSessionFactory rSessionFactory = defaultRSessionFactory();
		var worker1 = new SimpleRWorker(session -> session.eval("sessionInfo()"));
		var worker2 = new SimpleRWorker(session -> session.eval("sessionInfo()"));
		var waitWorker = waitWorker(10, TimeUnit.MINUTES);
		RWorkers.runWithinRsession(worker1, rSessionFactory);
		RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.builder()
			.requestedTimeAndUnit(1l, TimeUnit.SECONDS)
			.build());
		RWorkers.runWithinRsession(worker2, rSessionFactory);
		RWorkers.wait(5, TimeUnit.MINUTES, worker1, worker2, waitWorker);
	}
	
	@Test
	public void testParallelSessionRequest() throws Exception{
		int numberOfResources = 4;
		RSessionFactory rSessionFactory = new RSessionFactory();
		rSessionFactory.setAvailableInstances(Arrays.asList(localInstanceWithResources(numberOfResources)));
		List<SimpleRWorker> workers = new ArrayList<SimpleRWorker>();
		for(int i = 0; i<numberOfResources; i++){
			var waitWorker = waitWorker(45, TimeUnit.SECONDS);
			workers.add(waitWorker);
			RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		}
		RWorkers.wait(60, TimeUnit.SECONDS, workers.toArray(new SimpleRWorker[]{}));
	}
	
	@Test
	public void testParallel2SessionRequest() throws Exception{
		int numberOfResources = 2;
		RSessionFactory rSessionFactory = new RSessionFactory();
		rSessionFactory.setAvailableInstances(Arrays.asList(localInstanceWithResources(numberOfResources)));
		List<SimpleRWorker> workers = new ArrayList<SimpleRWorker>();
		for(int i = 0; i<numberOfResources; i++){
			var waitWorker = waitWorker(15, TimeUnit.SECONDS);
			workers.add(waitWorker);
			RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		}
		var finalWorker = waitWorker(15, TimeUnit.SECONDS);
		finalWorker.runAndWait(rSessionFactory, RSessionRequest.builder()
			.requestedTimeAndUnit(20l, TimeUnit.SECONDS)
			.build()
		);
		Assert.assertTrue(finalWorker.isFinished());
	}
	
	@Test
	public void testParallel4SessionRequest10() throws Exception{
		int numberOfResources = 4;
		RSessionFactory rSessionFactory = new RSessionFactory();
		rSessionFactory.setAvailableInstances(Arrays.asList(localInstanceWithResources(numberOfResources)));
		List<SimpleRWorker> workers = new ArrayList<SimpleRWorker>();
		for(int i = 0; i<10; i++){
			SimpleRWorker waitWorker = waitWorker(15, TimeUnit.SECONDS);
			workers.add(waitWorker);
			RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		}
		RWorkers.wait(20l, TimeUnit.MINUTES, workers.toArray(new SimpleRWorker[]{}));
	}
	
	private static RServerInstance localInstanceWithResources(int resourceNumber){
		return RServerInstance.builder()
			.resources(resourceNumber)
			.build();
	}
	
	private SimpleRWorker waitWorker(long time, TimeUnit unit) {
		return new SimpleRWorker(session -> {
			Stopwatch stopwatch = Stopwatch.createStarted();
			log.info("Starting sleeping worker");
			session.eval("sessionInfo()");
			Thread.sleep(unit.toMillis(time));
			log.info("Sleeping worker finished after {}", stopwatch);
		});
	}

}
