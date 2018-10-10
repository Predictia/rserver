package es.predictia.rserver;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("necesita R con paquete RServer instalado en local")
public class RWorkerTest {
	
	@Test(expected=ExecutionException.class)
	public void testClosingRWorker() throws Exception{
		final AbstractRWorker failingWorker = new AbstractRWorker() {
			@Override
			protected void runWithinSession(Rsession session) throws Throwable {
				throw new RuntimeException();
			}
		};
		failingWorker.runAndWait(new RSessionFactory(), new RSessionRequest.Builder().createRequest());
	}
		
	@Test(expected=ExecutionException.class)
	public void testFailingRWorker() throws Exception{
		final AbstractRWorker failingWorker = new AbstractRWorker() {
			@Override
			protected void runWithinSession(Rsession session) throws Throwable {
				session.eval("save()");
			}
		};
		failingWorker.runAndWait(new RSessionFactory(), new RSessionRequest.Builder().createRequest());
	}
	
	@Test
	public void testSourceRWorker() throws Exception{
		RScriptWorker scriptWorker = new RScriptWorker();
		scriptWorker.addLine("sessionInfo()");
		scriptWorker.runAndWait(new RSessionFactory(), new RSessionRequest.Builder().createRequest());
		Assert.assertFalse(scriptWorker.anyErrors());
		Assert.assertTrue(scriptWorker.isFinished());
	}
	
	@Test(expected=ExecutionException.class)
	public void testFailingSourceWorker() throws Exception{
		RScriptWorker scriptWorker = new RScriptWorker();
		scriptWorker.addLine("save()");
		scriptWorker.runAndWait(new RSessionFactory(), new RSessionRequest.Builder().createRequest());
	}

	@Test
	public void testLoadPackageRWorker() throws Exception{
		final AbstractRWorker failingWorker = new AbstractRWorker() {
			@Override
			protected void runWithinSession(Rsession session) throws Throwable {
				session.loadPackage("devtools");
			}
		};
		failingWorker.runAndWait(new RSessionFactory(), new RSessionRequest.Builder().createRequest());
	}
	
	@Test(expected=ExecutionException.class)
	public void testFailingLoadPackageRWorker() throws Exception{
		final AbstractRWorker failingWorker = new AbstractRWorker() {
			@Override
			protected void runWithinSession(Rsession session) throws Throwable {
				session.loadPackage("aaklsgjalfjalsf");
			}
		};
		failingWorker.runAndWait(new RSessionFactory(), new RSessionRequest.Builder().createRequest());
	}
	
}
