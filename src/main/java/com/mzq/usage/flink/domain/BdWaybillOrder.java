package com.mzq.usage.flink.domain;

import lombok.Data;

import java.util.Date;

@Data
public class BdWaybillOrder {

    private String waybillCode;
    private String waybillSign;
    private String siteCode;
    private String siteName;
    private String busiNo;
    private String busiName;
    private String sendPay;
    private Date pickupDate;
    private Date deliveryDate;
    private String orderCode;
    private Date orderCreateDate;
    private String packageCode;
}
