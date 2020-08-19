package com.mzq.usage.flink.domain;

import lombok.Data;

@Data
public class WaybillE {
    private String waybillCode;
    private String busiNo;
    private String busiName;
    private String sendPay;
    private Long timeStamp;
    private Long watermark;
}
