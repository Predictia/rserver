package es.predictia.rserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Runs {@link RWorker} objects using a {@link RSessionFactory}
 * @author Max
 *
 */
class RWorkers {

	/** Runs {@link RWorker} worker within session using a {@link RSessionRequest#createDefaultRequest()}
	 */
	public static CompletableFuture<RWorker> runWithinRsession(RWorker worker, RSessionFactory sessionFactory) throws Exception {
		return runWithinRsession(worker, sessionFactory, RSessionRequest.createDefaultRequest());
	}
	
	/** Runs {@link RWorker} worker within R session provided by the {@link RSessionFactory} 
	 */
	public static CompletableFuture<RWorker> runWithinRsession(RWorker worker, RSessionFactory sessionFactory, RSessionRequest sessionRequest) throws Exception {
		SessionTasks sessionTasks = new SessionTasks(sessionRequest, worker, sessionFactory);
		return sessionTasks.launchWorker();
	}
	
	/** Waits from the time of invocation for all the workers to finish, with a timeout
	 * @param time
	 * @param unit
	 * @param workers
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	static void wait(long time, TimeUnit unit, final SimpleRWorker... workers) throws TimeoutException, InterruptedException, ExecutionException {
		if(workers == null) return;
		if(allFinished(workers)) return;
		var waiters = Stream.of(workers)
			.map(worker -> CompletableFuture.runAsync(() -> {
				while(!worker.isFinished()) {
					try{
						Thread.sleep(5000l);
					}catch(InterruptedException ex){
						return;
					}
				}
			}))
			.collect(Collectors.toList());
		CompletableFuture
			.allOf(waiters.toArray(s -> new CompletableFuture[s]))
			.get(time, unit);
	}
	
	
	private static boolean allFinished(RWorker... workers){
		for(var worker : workers){
			if(!worker.isFinished()){
				return false;
			}
		}
		return true;
	}
	
}
