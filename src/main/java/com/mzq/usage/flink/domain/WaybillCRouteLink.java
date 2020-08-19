package com.mzq.usage.flink.domain;

import lombok.Data;

import java.util.Date;

@Data
public class WaybillCRouteLink  {
    private String waybillCode;
    private String waybillSign;
    private String siteCode;
    private String siteName;
    private String packageCode;
    private Date staticDeliveryTime;
}
