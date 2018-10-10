package es.predictia.rserver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/** Runnable that dumps all the added lines to a file and sources them
 * @author Max
 *
 */
public class RScriptWorker extends AbstractRWorker {

	public interface RScriptLine{
		public String getLine();
	}
	
	private final StringBuilder sb = new StringBuilder();;
	private final Collection<RScriptLine> lines = new ArrayList<RScriptLine>();
	
	public RScriptWorker(RScriptLine... lines) {
		super();
		if(lines != null){
			for(RScriptLine line : lines){
				addLine(line);
			}
		}
	}

	public void addLine(String line){
		this.lines.add(createLine(line));
	}
	
	private static RScriptLine createLine(final String rLine){
		return new RScriptLine() {
			@Override
			public String getLine() {
				return rLine;
			}
		};
	}
	
	public void addLine(Map<String, String> valuesMap, String line){
		this.lines.add(createLineReplacing(valuesMap, line));
	}
	
	private static RScriptLine createLineReplacing(final Map<String, String> valuesMap, final String rLine){
		return createLine(new StrSubstitutor(valuesMap).replace(rLine));
	}
	
	public void addLine(RScriptLine line){
		this.lines.add(line);
	}

	@Override
	protected void runWithinSession(Rsession s) throws Throwable {
		if(createScriptFile()){
			s.source(scriptFile);
			s.removeFile(scriptFile.getName());
			scriptFile.delete();
		}
	}
	
	private File scriptFile = null;
	
	private boolean createScriptFile() throws IOException{
		if (lines != null) {
			for (RScriptLine line : lines) {
				String rLine = line.getLine();
				LOGGER.debug("added line: {}", rLine);
				sb.append(rLine);
				sb.append(System.getProperty("line.separator"));
			}
		}
		if(sb.length() == 0l){
			return false;
		}
		scriptFile = File.createTempFile("script-", ".R");
		Files.write(sb, scriptFile, Charsets.UTF_8);
		return true;
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RScriptWorker.class);

}
