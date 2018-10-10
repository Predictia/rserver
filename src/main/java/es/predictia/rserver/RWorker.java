package es.predictia.rserver;

public interface RWorker {

	public void run(Rsession session) throws Throwable;
	
}
