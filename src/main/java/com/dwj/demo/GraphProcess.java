package com.dwj.demo;

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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.List;

/**
 * @author dwj
 * @date 2020/11/11 16:04
 */
public class GraphProcess {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Node> nodes = Node.getTestNodes(100);
        DataStream<Node> nodeStream = environment.fromCollection(nodes);
        DataStream<Edge> edgeStream = environment.fromCollection(Edge.getTestEdges(nodes));

        nodeStream.process(new NodeProcess());
        edgeStream.process(new EdgeActionProcess());

        submitJob();

        environment.execute();
    }

    private static void submitJob() throws Exception {
        System.out.println("Begin submit job");
//        String jar = "/home/chenwenqun/flink-demo-1.0.jar";
        String jar = "/Users/dwj/Sourcetree/myProject/flink-demo/target/flink-demo-1.0.jar";
        Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "10.0.60.128");
        config.setInteger(RestOptions.PORT, 8081);
        RestClusterClient<StandaloneClusterId> client = new RestClusterClient<>(config, StandaloneClusterId.getInstance());
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(new File(jar))
                .setEntryPointClassName("com.dwj.demo.Demo")
                .build();
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, 2, false);
        JobID jobId = client.submitJob(jobGraph).get();
        JobResult jobResult = client.requestJobResult(jobId).get();
        System.out.println(String.format("Jod submit success, Job id is %s", jobId));
        System.out.println("Jod submit success, Job: " + jobResult.getAccumulatorResults());
    }

    public static class NodeProcess extends ProcessFunction<Node, Node> {

        @Override
        public void processElement(Node node, Context context, Collector<Node> collector) throws Exception {
            // 判断当前节点状态：是否有数据、是否有告警
            System.out.println("Current node is: " + node.getId());
            node.setHaveAlert(true);
            node.setHaveData(true);
            collector.collect(node);
        }
    }

    public static class EdgeActionProcess extends ProcessFunction<Edge, Boolean> {

        @Override
        public void processElement(Edge edge, Context context, Collector<Boolean> collector) throws Exception {
            System.out.println(String.format("Current edge is: srt: %s, dst: %s", edge.getSrt(), edge.getDst()));
            collector.collect(true);
        }
    }
}
