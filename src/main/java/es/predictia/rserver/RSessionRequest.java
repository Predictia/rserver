package es.predictia.rserver;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Data;

@Data
public class RSessionRequest {
	
	private RSessionRequest(Integer requestedResources, 
			Long requestedTime, TimeUnit requestedTimeUnit,
			Long maxQueueTime, TimeUnit maxQueueTimeUnit
		) {
		super();
		this.requestedResources = requestedResources;
		this.requestedTime = requestedTime;
		this.requestedTimeUnit = requestedTimeUnit;
		this.maxQueueTime = maxQueueTime;
		this.maxQueueTimeUnit = maxQueueTimeUnit;
	}
	
	private final Integer requestedResources;
	private final Long requestedTime;
	private final TimeUnit requestedTimeUnit;
	private final Long maxQueueTime;
	private final TimeUnit maxQueueTimeUnit;
	
	private transient volatile RServerInstance instance;
	
	private transient LocalDateTime acceptedTime, requestTime;
	
	public static RSessionRequest createDefaultRequest(){
		return new Builder().createRequest();
	}
	
	public static class Builder{
		private Integer requestedResources = 1;
		private Long requestedTime = 24l;
		private TimeUnit requestedTimeUnit = TimeUnit.HOURS;
		private Long maxQueueTime = 24l;
		private TimeUnit maxQueueTimeUnit = TimeUnit.HOURS;
		
		public Builder withRequestedResources(Integer requestedResources) {
			this.requestedResources = requestedResources;
			return this;
		}
		public Builder withRequestedTime(Long requestedTime, TimeUnit requestedTimeUnit) {
			this.requestedTime = requestedTime;
			this.requestedTimeUnit = requestedTimeUnit;
			return this;
		}
		public Builder withMaxQueueTime(Long maxQueueTime, TimeUnit maxQueueTimeUnit) {
			this.maxQueueTime = maxQueueTime;
			this.maxQueueTimeUnit = maxQueueTimeUnit;
			return this;
		}
		public RSessionRequest createRequest(){
			return new RSessionRequest(requestedResources, requestedTime, requestedTimeUnit, maxQueueTime, maxQueueTimeUnit);
		}
	}

	@Override
	public String toString() {
		return "#" + this.hashCode();
	}
	
	static Predicate<RSessionRequest> predicateForInstance(RServerInstance instance){
		return input -> instance.equals(input.getInstance());
	}

	static final Function<RSessionRequest, Integer> TO_RESOURCES_FUNCTION = input -> input.getRequestedResources();

}
