package es.predictia.rserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/** Collection of R lines and auxiliary files
 * @author Max
 *
 */
@Data
@Builder
@Slf4j
public class RScript {

	@Singular
	private final List<String> lines;
	
	@Singular
	private final List<File> auxFiles;
	
	@Singular
	private final List<File> resultFiles;
	
	public static class RScriptBuilder {
		
		public RScriptBuilder lineReplacing(String line, Map<String, String> valuesMap){
			return line(new StringSubstitutor(valuesMap).replace(line));
		}
		
		public RScriptBuilder resource(InputStream is, Charset cs) throws IOException {
			var builder = this;
			try(var reader = new BufferedReader(new InputStreamReader(is, cs))) {
				for (String line = reader.readLine(); line != null; line = reader.readLine()) {
					builder = builder.line(line);
				}
			}
			return builder;
		}
		
		public RScriptBuilder resourceReplacing(InputStream is, Charset cs, Map<String, String> valuesMap) throws IOException {
			var builder = this;
			var subs = new StringSubstitutor(valuesMap);
			try(var reader = new BufferedReader(new InputStreamReader(is, cs))) {
				for (String line = reader.readLine(); line != null; line = reader.readLine()) {
					builder = builder.line(subs.replace(line));
				}
			}
			return builder;
		}
		
	}
	
	/** Worker that loads aux files, sources the lines, retrieves the result files and finally cleanups
	 * @return
	 * @throws IOException
	 */
	public RWorker toWorker() throws IOException {
		var scriptFile = File.createTempFile("script-", ".R");
		Files.write(scriptFile.toPath(), getLines(), StandardCharsets.UTF_8);
		log.debug("Writing script to: {}", scriptFile);
		getLines().forEach(log::debug);
		return new SimpleRWorker(s -> {
			for(File auxFile : auxFiles) {
				s.sendFile(auxFile);
			}
			s.source(scriptFile);
			s.removeFile(scriptFile.getName());
			for(File resultFile : resultFiles) {
				s.receiveFile(resultFile, resultFile.getName());;
				s.removeFile(resultFile.getName());
			}
		}, () -> scriptFile.delete());
	}
	
}
