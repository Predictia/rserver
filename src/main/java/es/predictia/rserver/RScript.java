package es.predictia.rserver;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;

/** Collection of R lines 
 * @author Max
 *
 */
@Data
@Builder
public class RScript {

	@Singular
	private final List<String> lines;
	
	public static class RScriptBuilder {
		
		public RScriptBuilder lineReplacing(String line, Map<String, String> valuesMap){
			return line(new StringSubstitutor(valuesMap).replace(line));
		}
		
	}
	
	/** Dumps all the added lines to a temporally file and sources them
	 * @return
	 * @throws IOException
	 */
	public RWorker toWorker() throws IOException {
		var scriptFile = File.createTempFile("script-", ".R");
		Files.write(scriptFile.toPath(), getLines(), StandardCharsets.UTF_8);
		return new SimpleRWorker(s -> {
			s.source(scriptFile);
			s.removeFile(scriptFile.getName());
		}, () -> scriptFile.delete());
	}
	
}
