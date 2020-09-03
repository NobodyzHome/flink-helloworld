package com.mzq.usage.flink.domain;

import lombok.Data;

import java.util.Date;

@Data
public class Order {
    private String orderCode;
    private Date createTime;
}
