package com.dwj.demo.task.dto;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author dwj
 * @date 2020/11/21 19:59
 */
@Data
@Accessors(chain = true)
public class Alert {
    private String id;
    private String name;
    private Long time;

    private Tags tags;

    @Data
    @Accessors(chain = true)
    public static class Tags {
        private String app;
        private String service;
        private String process;
        private String host;
    }
}
