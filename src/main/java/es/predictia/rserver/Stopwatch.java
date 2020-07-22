package es.predictia.rserver;

import java.time.Duration;
import java.time.Instant;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class Stopwatch {

	private final Instant start;

	public static Stopwatch createStarted() {
		return new Stopwatch(Instant.now());
	}

	public Duration getDuration() {
		return Duration.between(start, Instant.now());
	}

	public String toString() {
		return getDuration().toString();
	}

}
