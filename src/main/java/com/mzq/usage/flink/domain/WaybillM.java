package com.mzq.usage.flink.domain;

import lombok.Data;

import java.util.Date;

@Data
public class WaybillM {
    private String waybillCode;
    private Date pickupDate;
    private Date deliveryDate;
}
