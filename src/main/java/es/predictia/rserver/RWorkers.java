package es.predictia.rserver;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/** Runs {@link RWorker} objects using a {@link RSessionFactory}
 * @author Max
 *
 */
class RWorkers {

	/** Runs {@link RWorker} worker within session using a {@link RSessionRequest#createDefaultRequest()}
	 */
	public static FutureTask<RWorker> runWithinRsession(RWorker worker, RSessionFactory sessionFactory) throws Exception {
		return runWithinRsession(worker, sessionFactory, RSessionRequest.createDefaultRequest());
	}
	
	/** Runs {@link RWorker} worker within R session provided by the {@link RSessionFactory} 
	 */
	public static FutureTask<RWorker> runWithinRsession(RWorker worker, RSessionFactory sessionFactory, RSessionRequest sessionRequest) throws Exception {
		SessionTasks sessionTasks = new SessionTasks(sessionRequest, worker, sessionFactory);
		return sessionTasks.launchWorker(sessionFactory.getExecutorService());
	}
	
	/** Waits from the time of invocation for all the workers to finish, with a timeout
	 * @param time
	 * @param unit
	 * @param workers
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	static void wait(long time, TimeUnit unit, final SimpleRWorker... workers) throws InterruptedException, ExecutionException{
		if(workers == null) return;
		if(allFinished(workers)) return;
		Timeouts.sleepWithTimeOut(1000, new Timeouts.AwakeningCondition() {
			@Override
			public boolean wakeUp() throws Exception {
				return allFinished(workers);
			}
		}, time, unit);
	}
	
	private static boolean allFinished(SimpleRWorker... workers){
		for(SimpleRWorker worker : workers){
			if(!worker.isFinished()){
				return false;
			}
		}
		return true;
	}
	
}
