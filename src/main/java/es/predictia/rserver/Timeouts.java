package es.predictia.rserver;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class Timeouts {

	@FunctionalInterface
	public interface AwakeningCondition{
		public boolean wakeUp() throws Exception;
	}
	
	/** Sleeps checking for an {@link AwakeningCondition}, with a maximum sleeping time (timeOut)
	 * @param sleepInterval time to sleep (in millis) untill checking for {@link AwakeningCondition#wakeUp()}
	 * @param condition
	 * @param timeOut
	 * @param timeOutUnit
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void sleepWithTimeOut(final long sleepInterval, final AwakeningCondition condition, long timeOut, TimeUnit timeOutUnit) throws InterruptedException, ExecutionException{
		Exception e = Timeouts.callWithTimeOut(() -> {
			try{
				do{
					Thread.sleep(sleepInterval);
				}while(!condition.wakeUp());
			}catch(Exception ex){
				log.warn("Exception while waiting for process: " + ex.getMessage());
				return ex;
			}
			return null;
		}, timeOut, timeOutUnit);
		if(e instanceof InterruptedException){
			throw (InterruptedException) e;
		}else if(e instanceof Exception){
			throw new ExecutionException(e);
		}
	}
	
	/**
	 * @throws InterruptedException if timeout happened
	 * @throws ExecutionException 
	 */
	public static <T> T callWithTimeOut(Callable<T> callable, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException{
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<T> future = executorService.submit(callable);
		try{
			executorService.shutdown();
			if(!executorService.awaitTermination(timeout, unit)){
				throw new InterruptedException("Timeout after " + timeout + " " + unit);
			}
		}finally {
			executorService.shutdownNow();
		}
		return future.get();
	}
	
	/**
	 * @param task
	 * @param timeout
	 * @param unit
	 * @throws InterruptedException if timeout happened
	 */
	public static void runWithTimeOut(Runnable task, long timeout, TimeUnit unit) throws InterruptedException{
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		executorService.submit(task);
		try{
			executorService.shutdown();
			if(!executorService.awaitTermination(timeout, unit)){
				throw new InterruptedException("Timeout after " + timeout + " " + unit);
			}
		}finally {
			executorService.shutdownNow();
		}
	}
	
}