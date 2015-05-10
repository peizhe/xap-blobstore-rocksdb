package com.gigaspaces.blobstore.rocksdb.utils;

import com.gigaspaces.server.blobstore.BlobStoreException;

import java.io.File;
import java.util.Map;


/**
 * Created by kobi on 3/26/14.
 */
public class Utils {

    public static void runCommand(String command, File workingDir) {
        runCommand(command, workingDir, null);
    }

    public static void runCommand(String command, File workingDir, final Map<String, String> additionalProcessVariables) {
        String cmdLine = command;

        final String[] parts = cmdLine.split(" ");
        final ProcessBuilder pb = new ProcessBuilder(parts);

        if(workingDir != null){
            if(!workingDir.exists() || !workingDir.isDirectory()){
                throw new BlobStoreException(workingDir + " should be a path of a directory, please check if it exists");
            }
            pb.directory(workingDir);
        }
        if(additionalProcessVariables != null){
            Map<String, String> env = pb.environment();
            for(Map.Entry<String, String> additionalVar : additionalProcessVariables.entrySet()){
                env.put(additionalVar.getKey(), additionalVar.getValue());
            }
        }
        try{
            final Process process = pb.start();
            int errorCode = process.waitFor();
            if(errorCode != 0)
                throw new Exception("Failed to run command: " + command + " at directory: " + workingDir +" error code: "+errorCode);
        }catch (Exception e){
            throw new BlobStoreException("Failed to run command: " + command + " at directory: " + workingDir, e);
        }
    }
}
