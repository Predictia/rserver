package es.predictia.rserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang3.StringUtils;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Rsession implements AutoCloseable {

	private final RServerInstance instance;
	private final RConnection connection;

	public Rsession(RServerInstance instance) throws RserveException {
		this.instance = instance;
		this.connection = RServerInstance.connect(instance);
		if (this.connection == null) {
			throw new RuntimeException();
		} else if (!this.connection.isConnected()) {
			throw new RuntimeException();
		} else if (this.connection.getServerVersion() < MinRserveVersion) {
			throw new UnsupportedOperationException("Rserver needs to be at least" + MinRserveVersion);
		}else{
			log.info("Created connection with {}", instance);
		}
	}

	private final static int MinRserveVersion = 103;
	
	RConnection getConnection() {
		return connection;
	}
	
	public REXP loadPackage(String pack) throws REngineException, REXPMismatchException {
		return eval("library(" + pack + ")");
	}
	
	public REXP eval(String expression) throws REngineException, REXPMismatchException {
		if (StringUtils.isBlank(expression)) {
			return null;
		}
		log.debug("Eval expression: {}", expression);
		REXP e = null;
		synchronized (connection) {
			e = connection.parseAndEval(expression);
		}
		return e;
	}

	/**
	 * sends and sources a R source file (eg ".R" file)
	 *
	 * @param f ".R" file to source
	 * @throws REXPMismatchException
	 * @throws REngineException
	 * @throws IOException
	 */
	public void source(File f) throws REngineException, REXPMismatchException, IOException {
		sendFile(f);
		eval("source('" + f.getName() + "')");
	}

	/**
	 * sends and loads an R data file (eg ".Rdata" file)
	 *
	 * @param f ".Rdata" file to load
	 * @throws REXPMismatchException
	 * @throws REngineException
	 * @throws IOException
	 */
	public void load(File f) throws REngineException, REXPMismatchException, IOException {
		sendFile(f);
		eval("load('" + f.getName() + "')");
	}

	/**
	 * Get file from R environment to user filesystem
	 *
	 * @param localfile
	 *            local filesystem file
	 * @param remoteFile
	 *            R environment file name
	 */
	public void receiveFile(File localfile, String remoteFile) throws IOException {
		if (localfile.exists()) {
			localfile.delete();
		}
		log.debug("Transferring {} to file {}", remoteFile, localfile.getAbsolutePath());
		synchronized (connection) {
			try(
				InputStream is = new BufferedInputStream(connection.openFile(remoteFile));
				OutputStream os = new BufferedOutputStream(new FileOutputStream(localfile));
			) {
				is.transferTo(os);
			}
		}
	}

	/**
	 * delete R environment file
	 *
	 * @param remoteFile filename to delete
	 */
	public void removeFile(String remoteFile) throws RserveException {
		log.debug("Removing {}", remoteFile);
		synchronized (connection) {
			connection.removeFile(remoteFile);
		}
	}

	/**
	 * Send user filesystem file in r environement (like data)
	 *
	 * @param localfile File to send
	 * @throws IOException
	 */
	public void sendFile(File localfile) throws IOException {
		sendFile(localfile, localfile.getName());
	}

	/**
	 * Send user filesystem file in r environement (like data)
	 *
	 * @param localfile File to send
	 * @param remoteFile filename in R env.
	 */
	public void sendFile(File localfile, String remoteFile) throws IOException {
		if (!localfile.exists()) {
			throw new FileNotFoundException(localfile.getAbsolutePath());
		}
		log.debug("Transferring file {} to {}", localfile.getAbsolutePath(), remoteFile);
		synchronized (connection) {
			try(
				OutputStream os = new BufferedOutputStream(connection.createFile(remoteFile));
				InputStream is = new BufferedInputStream(new FileInputStream(localfile));
			) {
				is.transferTo(os);
			}
		}
	}

	@Override
	public void close() {
		if (connection == null) {
			return;
		}
		log.info("Closing connection with {}", instance);
		connection.close();
	}

}