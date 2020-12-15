package com.dwj.demo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author dwj
 * @date 2020/11/11 16:01
 */
@Data
public class Node {
    private Long id;
    private String name;

    private Boolean haveData;
    private Boolean haveAlert;

    private Map<String, Object> complexProp;

    Node(Long id) {
        this.id = id;
    }

    public static List<Node> getTestNodes(int count) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(new Node(Integer.toUnsignedLong(i)));
        }
        return nodes;
    }
}
