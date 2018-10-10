package es.predictia.rserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

@Ignore("necesita R con paquete RServer instalado en local")
public class RSessionFactoryTest {
	
	@Test
	public void testDefaultSessionsRequest() throws Exception{
		RSessionFactory rSessionFactory = defaultRSessionFactory();
		final AbstractRWorker worker1 = new NoOpWorker();
		final AbstractRWorker worker2 = new NoOpWorker();
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
		AbstractRWorker worker1 = new NoOpWorker();
		AbstractRWorker worker2 = new NoOpWorker();
		AbstractRWorker waitWorker = new WaitWorker(10, TimeUnit.MINUTES);
		RWorkers.runWithinRsession(worker1, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.runWithinRsession(worker2, rSessionFactory, RSessionRequest.createDefaultRequest());
		RWorkers.wait(15, TimeUnit.SECONDS, worker1, worker2, waitWorker);
	}

	@Test
	public void testExpiringSessionRequest() throws Exception{
		RSessionFactory rSessionFactory = defaultRSessionFactory();
		AbstractRWorker worker1 = new NoOpWorker();
		AbstractRWorker worker2 = new NoOpWorker();
		AbstractRWorker waitWorker = new WaitWorker(10, TimeUnit.MINUTES);
		RWorkers.runWithinRsession(worker1, rSessionFactory);
		RWorkers.runWithinRsession(waitWorker, rSessionFactory, new RSessionRequest.Builder()
			.withRequestedTime(1l, TimeUnit.SECONDS)
			.createRequest());
		RWorkers.runWithinRsession(worker2, rSessionFactory);
		RWorkers.wait(5, TimeUnit.MINUTES, worker1, worker2, waitWorker);
	}
	
	@Test
	public void testParallelSessionRequest() throws Exception{
		int numberOfResources = 4;
		RSessionFactory rSessionFactory = new RSessionFactory();
		rSessionFactory.setAvailableInstances(Lists.newArrayList(localInstanceWithResources(numberOfResources)));
		List<AbstractRWorker> workers = new ArrayList<AbstractRWorker>();
		for(int i = 0; i<numberOfResources; i++){
			AbstractRWorker waitWorker = new WaitWorker(45, TimeUnit.SECONDS);
			workers.add(waitWorker);
			RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		}
		RWorkers.wait(60, TimeUnit.SECONDS, workers.toArray(new AbstractRWorker[]{}));
	}
	
	@Test
	public void testParallel2SessionRequest() throws Exception{
		int numberOfResources = 2;
		RSessionFactory rSessionFactory = new RSessionFactory();
		rSessionFactory.setAvailableInstances(Lists.newArrayList(localInstanceWithResources(numberOfResources)));
		List<AbstractRWorker> workers = new ArrayList<AbstractRWorker>();
		for(int i = 0; i<numberOfResources; i++){
			AbstractRWorker waitWorker = new WaitWorker(15, TimeUnit.SECONDS);
			workers.add(waitWorker);
			RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		}
		WaitWorker finalWorker = new WaitWorker(15, TimeUnit.SECONDS);
		finalWorker.runAndWait(rSessionFactory, new RSessionRequest.Builder()
			.withRequestedTime(20l, TimeUnit.SECONDS)
			.createRequest()
		);
		Assert.assertTrue(finalWorker.isFinished());
	}
	
	@Test
	public void testParallel4SessionRequest10() throws Exception{
		int numberOfResources = 4;
		RSessionFactory rSessionFactory = new RSessionFactory();
		rSessionFactory.setAvailableInstances(Lists.newArrayList(localInstanceWithResources(numberOfResources)));
		List<AbstractRWorker> workers = new ArrayList<AbstractRWorker>();
		for(int i = 0; i<10; i++){
			AbstractRWorker waitWorker = new WaitWorker(15, TimeUnit.SECONDS);
			workers.add(waitWorker);
			RWorkers.runWithinRsession(waitWorker, rSessionFactory, RSessionRequest.createDefaultRequest());
		}
		RWorkers.wait(20l, TimeUnit.MINUTES, workers.toArray(new AbstractRWorker[]{}));
	}
	
	private static RServerInstance localInstanceWithResources(int resourceNumber){
		RServerInstance instance = new RServerInstance();
		instance.setResources(resourceNumber);
		return instance;
	}
	
	private static class WaitWorker extends AbstractRWorker{
		
		private final long time;
		
		private final TimeUnit unit;
		
		public WaitWorker(long time, TimeUnit unit) {
			super();
			this.time = time;
			this.unit = unit;
		}

		@Override
		protected void runWithinSession(final Rsession session) throws Throwable {
			Stopwatch stopwatch = Stopwatch.createStarted();
			LOGGER.info("Starting sleeping worker");
			session.eval("sessionInfo()");
			Thread.sleep(unit.toMillis(time));
			LOGGER.info("Sleeping worker finished after {}", stopwatch);
		}
		
	}
	
	private static class NoOpWorker extends AbstractRWorker{
		@Override
		protected void runWithinSession(Rsession session) throws REngineException, REXPMismatchException {
			session.eval("sessionInfo()");
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(RSessionFactoryTest.class);
	
}
