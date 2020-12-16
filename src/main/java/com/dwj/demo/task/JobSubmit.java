package com.dwj.demo.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;

import java.io.File;

/**
 * @author dwj
 * @date 2020/12/2 19:10
 */
@Slf4j
public class JobSubmit {

    public static void main(String[] args) throws Exception {
        submitJob();
    }

    public static void submitJob() throws Exception {
        log.info("Submit job");
        String jar = "/Users/dwj/Sourcetree/bizseer/troubleshoot/xts-task/target/xts-task-0.0.1-SNAPSHOT.jar";
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "10.0.60.128");
        config.setInteger(RestOptions.PORT, 8081);
        RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(config, StandaloneClusterId.getInstance());
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(new File(jar))
                .setEntryPointClassName("com.bizseer.xts.TroubleshootJob")
                .build();
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, 2, false);
        JobID jobId = client.submitJob(jobGraph).get();
        JobResult jobResult = client.requestJobResult(jobId).get();
        log.info(String.format("Jod submit success, Job id is %s", jobId));
        log.info("Jod submit success, Job: " + jobResult.getAccumulatorResults());
    }
}
