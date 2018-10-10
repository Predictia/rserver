package es.predictia.rserver;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

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
	
	private transient DateTime acceptedTime, requestTime;
	
	public DateTime getAcceptedTime() {
		return acceptedTime;
	}
	void setAcceptedTime(DateTime acceptedTime) {
		this.acceptedTime = acceptedTime;
	}
	public DateTime getRequestTime() {
		return requestTime;
	}
	void setRequestTime(DateTime requestTime) {
		this.requestTime = requestTime;
	}
	RServerInstance getInstance() {
		return instance;
	}
	void setInstance(RServerInstance instance) {
		this.instance = instance;
	}
	public Integer getRequestedResources() {
		return requestedResources;
	}
	public Long getRequestedTime() {
		return requestedTime;
	}
	public TimeUnit getRequestedTimeUnit() {
		return requestedTimeUnit;
	}
	public Long getMaxQueueTime() {
		return maxQueueTime;
	}
	public TimeUnit getMaxQueueTimeUnit() {
		return maxQueueTimeUnit;
	}
	
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
	
	static Predicate<RSessionRequest> predicateForInstance(final RServerInstance instance){
		return new Predicate<RSessionRequest>() {
			@Override
			public boolean apply(RSessionRequest input) {
				return instance.equals(input.getInstance());
			}
		};
	}
	

	static final Function<RSessionRequest, Integer> TO_RESOURCES_FUNCTION = new Function<RSessionRequest, Integer>() {
		@Override
		public Integer apply(RSessionRequest input) {
			return input.getRequestedResources();
		}
	};

}
