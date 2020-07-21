package es.predictia.rserver;

import java.util.concurrent.Future;

public interface RWorker extends AutoCloseable {

	public void run(Rsession session) throws Throwable;
	
	public boolean isStarted();
	
	public boolean isFinished();

	public boolean anyErrors();
	
	/** Submits worker with {@link RSessionRequest} to the {@link RSessionFactory} and waits till it finishes
	 * @param sessionFactory
	 * @param request
	 * @throws Exception if any error occurs 
	 */
	public default void runAndWait(RSessionFactory sessionFactory, RSessionRequest request) throws Exception {
		run(sessionFactory, request).get();
	}
	
	/**
	 * Submits worker with {@link RSessionRequest} to the {@link RSessionFactory}
	 * @param sessionFactory
	 * @param request
	 * @throws Exception if any error occurs
	 */
	public default Future<RWorker> run(RSessionFactory sessionFactory, RSessionRequest request) throws Exception {
		return RWorkers.runWithinRsession(this, sessionFactory, request);
	}
	
}
