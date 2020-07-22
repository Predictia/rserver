package es.predictia.rserver;

import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;

/** Runs R code and then closes the session
 * @author Max
 *
 */
@RequiredArgsConstructor
public class SimpleRWorker implements RWorker {

	private final AtomicBoolean isStarted = new AtomicBoolean(false),
			isFinished = new AtomicBoolean(false),
			anyErrors = new AtomicBoolean(false);
	
	private final SessionConsumer sessionConsumer;
	
	public SimpleRWorker(SessionConsumer sessionConsumer) {
		this(sessionConsumer, null);
	}
	
	private final CloseHook closeHook;
	
	@FunctionalInterface
	public static interface SessionConsumer {
		void runWithinSession(Rsession session) throws Throwable;
	}
	
	@FunctionalInterface
	public static interface CloseHook {
		void close() throws Exception;
	}
	
	@Override
	public void run(Rsession session) throws Throwable {
		this.isStarted.set(true);
		try{
			sessionConsumer.runWithinSession(session);
		}catch(Throwable e){
			this.anyErrors.set(true);
			throw e;
		}finally{
			session.close();
			this.isFinished.set(true);
		}
	}

	@Override
	public boolean isStarted() {
		return isStarted.get();
	}

	@Override
	public boolean isFinished() {
		return isFinished.get();
	}

	@Override
	public boolean anyErrors() {
		return anyErrors.get();
	}

	@Override
	public void close() throws Exception {
		if(closeHook != null) {
			closeHook.close();
		}
	}
	
}
