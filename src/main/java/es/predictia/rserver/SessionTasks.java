package es.predictia.rserver;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Data
@Slf4j
class SessionTasks {
	
	private final RSessionRequest sessionRequest;
	private final RWorker worker;
	private final RSessionFactory sessionFactory;
	
	private CompletableFuture<Rsession> sessionFuture;
	private CompletableFuture<RWorker> workerFuture;

	public boolean isDone(){
		return workerFuture != null && workerFuture.isDone();
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
	
	public CompletableFuture<RWorker> launchWorker() {
		this.sessionFuture = CompletableFuture.supplyAsync(() -> {
			boolean first = true;
			Optional<RServerInstance> availableInstance = Optional.empty();
			Stopwatch stopwatch = Stopwatch.createStarted();
			try {
				while(!availableInstance.isPresent()){
					if(!first){
						Thread.sleep(POLL_INTERVAL);
						if(stopwatch.getDuration().toSeconds() > TimeUnit.SECONDS.convert(sessionRequest.getMaxQueueTime(), sessionRequest.getMaxQueueTimeUnit())){
							throw new Exception("RSessionRequest in queue for too long");
						}
					}
					availableInstance = sessionFactory.getInstanceForRequest(this);
					first = false;
				}
				return new Rsession(sessionRequest.getInstance());
			}catch (Exception e) {
				log.warn("Error while creating session", e);
				throw new RuntimeException(e);
			}
		}, sessionFactory.getExecutorService());
		this.workerFuture = sessionFuture.thenApplyAsync(rsession -> {
			try {
				worker.run(rsession);
				return worker;
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}, sessionFactory.getExecutorService());
		return this.workerFuture;
	}
	
	public static Predicate<SessionTasks> predicateForRequest(final RSessionRequest rSessionRequest){
		return input -> input.getSessionRequest().equals(rSessionRequest);
	}
	
	public static final Function<SessionTasks, RSessionRequest> TO_REQUEST_FUNCTION = input -> input.getSessionRequest();
	
	public static final Predicate<SessionTasks> DONE_PREDICATE = input -> input.isDone();
	
	private static final long POLL_INTERVAL = 5000l;
	
}