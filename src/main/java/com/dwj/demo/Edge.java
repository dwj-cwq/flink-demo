package com.dwj.demo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author dwj
 * @date 2020/11/11 16:03
 */
@Data
@Accessors(chain = true)
public class Edge {
    private Long srt;
    private Long dst;

    private Action action;

    private Map<String, Object> complexProp;

    Edge(Long srt, Long dst) {
        this.srt = srt;
        this.dst = dst;
    }

    public static List<Edge> getTestEdges(List<Node> nodes) {
        List<Edge> edges = new ArrayList<>();
        for (int i = 0; i < nodes.size() - 1; i++) {
            Edge edge = new Edge(nodes.get(i).getId(), nodes.get(i + 1).getId())
                    .setAction(new Action()
                            .setId(Integer.toUnsignedLong(i)));
            edges.add(edge);
        }
        return edges;
    }

    @Data
    public static class Action {
        private Long id;
        private String actionName;
        private String actionContent;
    }

}
