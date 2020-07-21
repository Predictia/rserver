package es.predictia.rserver;

import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RServerInstance {

	@Builder.Default
	private final String url = "R://localhost";
	
	@Builder.Default
	private final Integer resources = 1;
	
	static RConnection connect(RServerInstance instance) throws RserveException {
		String hostPort = hostPort(instance);
		String userPass = userPass(instance);
		String host = hostPort;
		Integer port = null;
		if (hostPort.contains(":")) {
			host = hostPort.split(":")[0];
			port = Integer.valueOf(hostPort.split(":")[1]);
		}
		RConnection connection;
		if (port != null) {
			connection = new RConnection(host, port);
		} else {
			connection = new RConnection(host);
		}
		if (connection.needLogin() && userPass != null) {
			String login = userPass;
			String password = null;
			if (userPass.contains(":")) {
				login = userPass.split(":")[0];
				password = userPass.split(":")[1];
			}
			connection.login(login, password);
		}
		return connection;
	}

	private static String userPass(RServerInstance instance) {
		String RURL = instance.getUrl();
		if (!RURL.startsWith(RURL_START)) {
			throw new IllegalArgumentException(RURL);
		}
		RURL = RURL.substring(RURL_START.length());
		if (RURL.contains("@")) {
			return RURL.split("@")[0];
		} else {
			return null;
		}
	}

	private static String hostPort(RServerInstance instance) {
		String RURL = instance.getUrl();
		if (!RURL.startsWith(RURL_START)) {
			throw new IllegalArgumentException(RURL);
		}
		RURL = RURL.substring(RURL_START.length());
		String hostPort = RURL;
		if (RURL.contains("@")) {
			hostPort = RURL.split("@")[1];
		}
		return hostPort;
	}

	private final static String RURL_START = "R://";

	@Override
	public String toString() {
		return url;
	}
	
}