package es.predictia.rserver;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class RSessionFactory  {

	private transient Collection<SessionTasks> sessionTasks = Collections.synchronizedSet(new HashSet<SessionTasks>());
	
	synchronized Optional<RServerInstance> getInstanceForRequest(SessionTasks tasks){
		expireSessions();
		deleteOldSessions();
		boolean newRequest = !sessionTasks.contains(tasks);
		RSessionRequest sessionRequest = tasks.getSessionRequest();
		if(newRequest){
			LOGGER.info("Received session request for "+ sessionRequest.getRequestedResources()  + " resources: " + sessionRequest);
			sessionTasks.add(tasks);
		}
		LOGGER.debug("Searching for available instances");
		for(RServerInstance instance : availableInstances){
			long usedResources = sum(FluentIterable
				.from(sessionTasks)
				.transform(SessionTasks.TO_REQUEST_FUNCTION)
				.filter(RSessionRequest.predicateForInstance(instance))
				.transform(RSessionRequest.TO_RESOURCES_FUNCTION)
			);
			if(instance.getResources() >= (usedResources + sessionRequest.getRequestedResources())){
				LOGGER.info("Found suitable instance for request " + sessionRequest + ": " + instance.getUrl());
				sessionRequest.setInstance(instance);
				sessionRequest.setAcceptedTime(new DateTime());
				return Optional.of(instance);
			}			
		}
		return Optional.absent();
	}

	
	private void expireSessions(){
		for(SessionTasks iSessionTasks : sessionTasks){
			iSessionTasks.cancelExpiredWorker();
		}
	}
	
	private void deleteOldSessions(){
		Collection<SessionTasks> oldSessions = FluentIterable
			.from(sessionTasks)
			.filter(SessionTasks.DONE_PREDICATE)
			.toList();
		if(oldSessions.isEmpty()){
			return;
		}
		LOGGER.debug("Cleaning old sessions");
		sessionTasks.removeAll(oldSessions);
	}
	
	private static long sum(Iterable<? extends Number> els){
		long res = 0;
		for(Number el : els){
			res += el.longValue();
		}
		return res;
	}
	
	private List<RServerInstance> availableInstances = Lists.newArrayList(new RServerInstance());
	
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
		long totalNumberOfResources = sum(FluentIterable.from(availableInstances).transform(new Function<RServerInstance, Integer>() {
			@Override
			public Integer apply(RServerInstance input) {
				return input.getResources();
			}
		}));
		return Executors.newFixedThreadPool(Long.valueOf(totalNumberOfResources).intValue() + 4);
	}
	
	public synchronized void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RSessionFactory.class);

}
