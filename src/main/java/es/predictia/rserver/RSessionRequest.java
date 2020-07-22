package es.predictia.rserver;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RSessionRequest {
	
	@Builder.Default
	private Integer requestedResources = 1;
	
	@Builder.Default
	private Long requestedTime = 24l;
	
	@Builder.Default
	private TimeUnit requestedTimeUnit = TimeUnit.HOURS;
	
	@Builder.Default
	private Long maxQueueTime = 24l;
	
	@Builder.Default
	private TimeUnit maxQueueTimeUnit = TimeUnit.HOURS;
	
	private transient volatile RServerInstance instance;
	
	private transient LocalDateTime acceptedTime, requestTime;
	
	public static RSessionRequest createDefaultRequest(){
		return RSessionRequest.builder().build();
	}
	
	public static class RSessionRequestBuilder{
		
		public RSessionRequestBuilder requestedTimeAndUnit(Long requestedTime, TimeUnit unit) {
			return requestedTime(requestedTime)
				.requestedTimeUnit(unit);
		}
		public RSessionRequestBuilder maxQueueTimeAndUnit(Long maxQueueTime, TimeUnit unit) {
			return maxQueueTime(maxQueueTime)
				.maxQueueTimeUnit(unit);
		}
		
	}

	@Override
	public String toString() {
		return "#" + this.hashCode();
	}
	
	static Predicate<RSessionRequest> predicateForInstance(RServerInstance instance){
		return input -> instance.equals(input.getInstance());
	}

}
