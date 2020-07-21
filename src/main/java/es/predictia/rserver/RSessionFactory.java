package es.predictia.rserver;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RSessionFactory  {

	private transient Collection<SessionTasks> sessionTasks = Collections.synchronizedSet(new HashSet<SessionTasks>());
	
	synchronized Optional<RServerInstance> getInstanceForRequest(SessionTasks tasks){
		expireSessions();
		deleteOldSessions();
		boolean newRequest = !sessionTasks.contains(tasks);
		RSessionRequest sessionRequest = tasks.getSessionRequest();
		if(newRequest){
			log.info("Received session request for "+ sessionRequest.getRequestedResources()  + " resources: " + sessionRequest);
			sessionTasks.add(tasks);
		}
		log.debug("Searching for available instances");
		for(RServerInstance instance : availableInstances){
			long usedResources = sessionTasks.stream()
				.map(SessionTasks.TO_REQUEST_FUNCTION)
				.filter(RSessionRequest.predicateForInstance(instance))
				.mapToInt(RSessionRequest::getRequestedResources)
				.sum();
			if(instance.getResources() >= (usedResources + sessionRequest.getRequestedResources())){
				log.info("Found suitable instance for request " + sessionRequest + ": " + instance.getUrl());
				sessionRequest.setInstance(instance);
				sessionRequest.setAcceptedTime(LocalDateTime.now());
				return Optional.of(instance);
			}			
		}
		return Optional.empty();
	}

	
	private void expireSessions(){
		for(SessionTasks iSessionTasks : sessionTasks){
			iSessionTasks.cancelExpiredWorker();
		}
	}
	
	private void deleteOldSessions(){
		Collection<SessionTasks> oldSessions = sessionTasks.stream()
			.filter(SessionTasks.DONE_PREDICATE)
			.collect(Collectors.toList());
		if(oldSessions.isEmpty()){
			return;
		}
		log.debug("Cleaning old sessions");
		sessionTasks.removeAll(oldSessions);
	}
	
	private List<RServerInstance> availableInstances = Arrays.asList(RServerInstance.builder().build());
	
	public void setAvailableInstances(List<RServerInstance> availableInstances) {
		this.availableInstances = availableInstances;
	}

	private ExecutorService executorService;
	
	ExecutorService getExecutorService() {
		if(executorService == null){
			setExecutorService(createDefaultService());
		}
		return executorService;
	}

	private synchronized ExecutorService createDefaultService() {
		long totalNumberOfResources = availableInstances.stream()
			.mapToLong(RServerInstance::getResources)
			.sum();
		return Executors.newFixedThreadPool(Long.valueOf(totalNumberOfResources).intValue() + 4);
	}
	
	public synchronized void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

}
