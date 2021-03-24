package com.qyf.mq.entity;

import lombok.Data;

import java.util.Date;

@Data
public class Content {

    private String id;

    private String content;

    private String state;

    private Date time;
}
