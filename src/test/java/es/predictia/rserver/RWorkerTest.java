package es.predictia.rserver;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

//@Ignore("necesita R con paquete RServer instalado en local")
public class RWorkerTest {
	
	@Test(expected = ExecutionException.class)
	public void testClosingRWorker() throws Exception{
		try(var failingWorker = new SimpleRWorker(session -> {throw new RuntimeException(); })){
			failingWorker.runAndWait(new RSessionFactory(), RSessionRequest.createDefaultRequest());
		}
	}
		
	@Test(expected = ExecutionException.class)
	public void testFailingRWorker() throws Exception{
		try(var failingWorker = new SimpleRWorker(session -> session.eval("save()"))){
			failingWorker.runAndWait(new RSessionFactory(), RSessionRequest.createDefaultRequest());
		}
	}
	
	@Test
	public void testSourceRWorker() throws Exception{
		try(var scriptWorker = RScript.builder()
			.line("sessionInfo()")
			.build()
			.toWorker()){
			scriptWorker.runAndWait(new RSessionFactory(), RSessionRequest.createDefaultRequest());
			Assert.assertFalse(scriptWorker.anyErrors());
			Assert.assertTrue(scriptWorker.isFinished());
		}
		
	}
	
	@Test(expected = ExecutionException.class)
	public void testFailingSourceWorker() throws Exception{
		try(var scriptWorker = RScript.builder()
				.line("save()")
				.build()
				.toWorker()){
			scriptWorker.runAndWait(new RSessionFactory(), RSessionRequest.createDefaultRequest());
		}
	}

	@Test
	public void testLoadPackageRWorker() throws Exception{
		try(var worker = new SimpleRWorker(session -> session.loadPackage("Rserve"))){
			worker.runAndWait(new RSessionFactory(), RSessionRequest.createDefaultRequest());
		}
	}
	
	@Test(expected = ExecutionException.class)
	public void testFailingLoadPackageRWorker() throws Exception{
		try(var failingWorker = new SimpleRWorker(session -> session.loadPackage("aaklsgjalfjalsf"))){
			failingWorker.runAndWait(new RSessionFactory(), RSessionRequest.createDefaultRequest());
		}
	}
	
}
