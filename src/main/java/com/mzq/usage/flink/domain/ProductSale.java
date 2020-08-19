package com.mzq.usage.flink.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class ProductSale implements Serializable {
    private String productName;
    private Double sale;
}