package com.dwj.demo.task.dto;

import lombok.Data;

/**
 * @author Chen Wenqun
 */
@Data
public class TicketDTO {

    private Long timestamp;

    private Long id;

    private String user;

    private Double price;
}
