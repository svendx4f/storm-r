package com.github.quintona;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class RFunction extends BaseFunction {
	
	private Process process;
    private DataOutputStream rInput;
    private String rExecutable;
    private List<String> libraries;
    private String functionName;

    private Queue<String> errors = new LinkedList<>();
    private Queue<String> responses = new LinkedList<>();
    private transient Executor exec;

    private static String ls = System.getProperty("line.separator");
	private String initCode = null;

	public static final String START_LINE = "<s>";
	public static final String END_LINE = "<e>";
	
	public RFunction(String rExecutable, List<String> libraries, String functionName){
		this.rExecutable = rExecutable;
		this.functionName = functionName;
		this.libraries = libraries;
	}
	
	public RFunction(List<String> libraries, String functionName){
		rExecutable = "/usr/bin/R";
		this.functionName = functionName;
		this.libraries = libraries;
	}
	
	public RFunction withInitCode(String rCode){
		this.initCode = rCode;
		return this;
	}
	
	public RFunction withNamedInitCode(String name){
		this.initCode = readFile("/" + name + ".R");
		return this;
	}
	
	public void prepare(Map conf, TridentOperationContext context) {
		ProcessBuilder builder = new ProcessBuilder(rExecutable, "--vanilla", "-q", "--slave");
		try {
			process = builder.start();
            exec = Executors.newFixedThreadPool(2);

            // I/O to R + async thread to listen to errors
			rInput = new DataOutputStream(process.getOutputStream());
            exec.execute(new RIOReader(process.getInputStream(), responses));
            exec.execute(new RIOReader(process.getErrorStream(), errors));

			loadLibraries();
			if(initCode != null){
				rInput.writeBytes(initCode + "\n");
				rInput.flush();
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not start R, please check install and settings" , e);
		}
    }
	
	private void loadLibraries() throws IOException{
		rInput.writeBytes("library('rjson')\n");
		for(String lib : libraries){
			rInput.writeBytes("library('"+ lib +"')\n");
		}
		rInput.flush();
	}
	
	public static String trimOutput(String output){
        if (output == null) return "";
		output = output.replace("[1]", "");
		output = output.replace("\\", "");
		output = output.trim();
		return output.substring(1, output.length() - 1);
	}
	
	private JSONArray getResult() throws ParseException{
		StringBuilder stringBuilder = new StringBuilder();

		boolean awaitingStart = true;
        checkErrors();

        while (responses.isEmpty()) {
            try {
                System.out.println("waiting answer from R...");
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        while (!responses.isEmpty()) {
            String line = responses.poll();

            if(line.equals(START_LINE)){
                awaitingStart = false;
            } else if(line.equals(END_LINE)) {
                if(awaitingStart)
                    throw new RuntimeException("Something went wrong. Received response ending before beginning!");
                break;
            } else if(!awaitingStart){
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        }

        final String response = stringBuilder.toString().trim();
        if(awaitingStart) {
            if (!"".equals(response))
                throw new RuntimeException("Unrecognized response from R runtime: " + response);
            return null;
        }
		final String trimmedContent = trimOutput(response);
		if(trimmedContent == null)
			return null;
		if("[]".equals(trimmedContent))
			return null;
        return (JSONArray)JSONValue.parseWithException(trimmedContent);
	}

    /** Checks the presence of any error reported by R and, if so, throws an exception **/
    private void checkErrors(){
        if (!errors.isEmpty()) {
            StringBuffer err = new StringBuffer();
            while (!errors.isEmpty()) {
                err.append(errors.poll());
            }
            throw new RuntimeException("Error from the R runtime: " + err);
        }

        try {
            throw new RuntimeException("R runtime has terminated with return value: " + process.exitValue());
        } catch (IllegalThreadStateException exc) {
            // NOP: the process has not terminated: things are cool
        }
    }

    public static String readFile(String file) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				RFunction.class.getResourceAsStream(file)));
		StringBuilder stringBuilder = new StringBuilder();

		try {
		    String line;
			while ((line = reader.readLine()) != null) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not load resource: ", e);
		}
		return stringBuilder.toString();
	}

    @Override
    public void cleanup() {
    	process.destroy();
    }
    
    public JSONArray coerceTuple(TridentTuple tuple){
    	JSONArray array = new JSONArray();
    	array.addAll(tuple);
    	return array;
    }
    
    public Values coerceResponce(JSONArray array){
    	return new Values(array.toArray());
    }
    
    public JSONArray performFunction(JSONArray functionInput){
    	try {

    		String input = functionInput.toJSONString().replace("\\", "");
			rInput.writeBytes("list <- fromJSON('" + input + "')\n");
			rInput.writeBytes("output <- " + functionName + "(list)\n");
			rInput.writeBytes("write('" + START_LINE + "', stdout())\n");
			rInput.writeBytes("toJSON(output)\n");
			rInput.writeBytes("write('" + END_LINE + "', stdout())\n");
			rInput.flush();

			return getResult();
		} catch (IOException | ParseException e) {
            checkErrors();
			throw new RuntimeException("Exception handling response from R" , e);
		}
    }

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		JSONArray functionInput = coerceTuple(tuple);
		JSONArray result = performFunction(functionInput);
		if(result != null)
			collector.emit(coerceResponce(result));
	}


    /**
     * async listener on a stream which simply makes everything available in a queue for later
     * */
    private class RIOReader implements Runnable {

        private final BufferedReader reader;
        private final Queue<String> msgs;

        private RIOReader(InputStream stream, Queue<String> msgs) {
            reader = new BufferedReader(new InputStreamReader(stream));
            this.msgs = msgs;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String s = reader.readLine();
                    if (s != null) {
                        msgs.add(s);
                    } else {
                        Thread.sleep(5);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("could not read stream from R runtime", e);
            } finally {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    };
}