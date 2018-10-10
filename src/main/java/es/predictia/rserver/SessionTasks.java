package es.predictia.rserver;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.rosuda.REngine.Rserve.RserveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;

class SessionTasks {
	
	private final RSessionRequest sessionRequest;
	private final RWorker worker;
	private final FutureTask<Rsession> sessionFuture;
	private final FutureTask<RWorker> workerFuture;
	
	public SessionTasks(final RSessionRequest sessionRequest, final RWorker worker, final RSessionFactory sessionFactory) {
		super();
		this.sessionRequest = sessionRequest;
		this.worker = worker;
		final SessionTasks sessionTasks = this;
		this.sessionFuture = new FutureTask<Rsession>(new Callable<Rsession>() {
			@Override
			public Rsession call() throws Exception {
				boolean first = true;
				Optional<RServerInstance> availableInstance = Optional.absent();
				Stopwatch stopwatch = Stopwatch.createStarted();
				while(!availableInstance.isPresent()){
					if(!first){
						Thread.sleep(POLL_INTERVAL);
						if(stopwatch.elapsed(sessionRequest.getMaxQueueTimeUnit()) > sessionRequest.getMaxQueueTime()){
							throw new Exception("RSessionRequest in queue for too long");
						}
					}
					availableInstance = sessionFactory.getInstanceForRequest(sessionTasks);
					first = false;
				}
				Rsession s = createRsession(sessionRequest);
				return s;
			}
		});
		this.workerFuture = new FutureTask<RWorker>(new Callable<RWorker>() {
			@Override
			public RWorker call() throws Exception {
				while(!sessionFuture.isDone()){
					Thread.sleep(POLL_INTERVAL);
				}
				Rsession rsession = sessionFuture.get();
				try {
					worker.run(rsession);
				} catch (Throwable e) {
					throw new RuntimeException(e);
				}
				return worker;
			}
		});
	}

	private Rsession createRsession(RSessionRequest sessionRequest) throws RserveException{
		final Rsession s = new Rsession(sessionRequest.getInstance());
		return s;
	}

	public boolean isDone(){
		if(sessionFuture == null){
			return false;
		}else if(workerFuture == null){
			return false;
		}else{
			return sessionFuture.isDone() && workerFuture.isDone();
		}
	}
	
	public boolean cancelExpiredWorker(){
		if(sessionRequest.getAcceptedTime() == null){
			return false;
		}else if(workerFuture == null){
			return false;
		}else if(workerFuture.isDone()){
			return false;
		}
		Optional<Rsession> oSession = getRSession();
		if(!oSession.isPresent()){
			return false;
		}
		Long reqTime = sessionRequest.getRequestedTime();
		TimeUnit reqTimeUnit = sessionRequest.getRequestedTimeUnit();
		Duration jobDuration = new Interval(sessionRequest.getAcceptedTime(), new DateTime()).toDuration();
		if(jobDuration.isLongerThan(Duration.standardSeconds(TimeUnit.SECONDS.convert(reqTime, reqTimeUnit)))){
			LOGGER.debug("Session: " + sessionRequest + " exhausted its time");
			oSession.get().close();
			workerFuture.cancel(true);
			return true;
		}else{
			LOGGER.debug("Session: {} still in time: {}", sessionRequest, jobDuration);
			return false;
		}
	}
	
	private Optional<Rsession> getRSession(){
		if(sessionFuture == null){
			return Optional.absent();
		}else if(!sessionFuture.isDone()){
			return Optional.absent();
		}
		try{
			return Optional.of(sessionFuture.get());
		}catch(Exception e){
			LOGGER.debug("Session {} could not be created", sessionRequest);
			return Optional.absent();
		}
	}
	
	public boolean isSessionConnected(){
		Optional<Rsession> oSession = getRSession();
		if(!oSession.isPresent()){
			return false;
		}
		Rsession rsession = oSession.get();
		if(rsession.getConnection() != null){
			boolean result = rsession.getConnection().isConnected();
			if(!result){
				LOGGER.debug("Session {} is not connected", sessionRequest);
			}
			return result;
		}else{
			LOGGER.debug("Session {} has no connection attached", sessionRequest);
			return false;
		}
	}

	public RSessionRequest getSessionRequest() {
		return sessionRequest;
	}
	public RWorker getWorker() {
		return worker;
	}
	
	public FutureTask<RWorker> launchWorker(ExecutorService executorService) {
		executorService.submit(this.sessionFuture);
		executorService.submit(this.workerFuture);
		return workerFuture;
	}
	
	public static Predicate<SessionTasks> predicateForRequest(final RSessionRequest rSessionRequest){
		return new Predicate<SessionTasks>() {
			@Override
			public boolean apply(SessionTasks input) {
				return input.getSessionRequest().equals(rSessionRequest);
			}
		};
	}
	
	public static final Function<SessionTasks, RSessionRequest> TO_REQUEST_FUNCTION = new Function<SessionTasks, RSessionRequest>() {
		@Override
		public RSessionRequest apply(SessionTasks input) {
			return input.getSessionRequest();
		}
	};
	
	public static final Predicate<SessionTasks> DONE_PREDICATE = new Predicate<SessionTasks>() {
		@Override
		public boolean apply(SessionTasks input) {
			return input.isDone();
		}
	};
	
	private static final long POLL_INTERVAL = 5000l;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SessionTasks.class);
	
}