package es.predictia.rserver;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.rosuda.REngine.Rserve.RserveException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
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
		this.sessionFuture = new FutureTask<Rsession>(() -> {
			boolean first = true;
			Optional<RServerInstance> availableInstance = Optional.empty();
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
		});
		this.workerFuture = new FutureTask<RWorker>(() -> {
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
		Duration jobDuration = Duration.between(sessionRequest.getAcceptedTime(), LocalDateTime.now());
		if(jobDuration.toSeconds() > TimeUnit.SECONDS.convert(reqTime, reqTimeUnit)){
			log.debug("Session: " + sessionRequest + " exhausted its time");
			oSession.get().close();
			workerFuture.cancel(true);
			return true;
		}else{
			log.debug("Session: {} still in time: {}", sessionRequest, jobDuration);
			return false;
		}
	}
	
	private Optional<Rsession> getRSession(){
		if(sessionFuture == null){
			return Optional.empty();
		}else if(!sessionFuture.isDone()){
			return Optional.empty();
		}
		try{
			return Optional.of(sessionFuture.get());
		}catch(Exception e){
			log.debug("Session {} could not be created", sessionRequest);
			return Optional.empty();
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
				log.debug("Session {} is not connected", sessionRequest);
			}
			return result;
		}else{
			log.debug("Session {} has no connection attached", sessionRequest);
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
		return input -> input.getSessionRequest().equals(rSessionRequest);
	}
	
	public static final Function<SessionTasks, RSessionRequest> TO_REQUEST_FUNCTION = new Function<SessionTasks, RSessionRequest>() {
		@Override
		public RSessionRequest apply(SessionTasks input) {
			return input.getSessionRequest();
		}
	};
	
	public static final Predicate<SessionTasks> DONE_PREDICATE = input -> input.isDone();
	
	private static final long POLL_INTERVAL = 5000l;
	
}