package com.mzq.usage.flink;

import org.junit.Test;

public class JacksonTest {


    /**
     * 测试读
     * <p>
     * 1.测试数组属性
     * 2.测试Map属性
     * 3.测试根据时间戳（数字形）转Date
     * 4.测试根据时间戳（标准字符串形2020-11-24T06:21:05.000Z）转Date
     * 5.测试根据无时区的时间转Date
     * 6.测试数组形式的json
     * 7.测试List属性
     * 8.json串中属性值是数字，但是对应的pojo类中的属性是String类型
     * 9.json串中属性值是仅有一个值的数组，把它映射成POJO类的非集合类型的属性中（例如json串中属性a的值是["1"]，要映射到的pojo类中a的属性的类型是String，而不是String[]）
     * 10.json串中属性值是一个非数组类型的值（例如string），把它映射成POJO类的集合类型的属性中（例如json串中属性a的值是"1"，要映射到的pojo类中a的属性的类型是List，而不是String）
     * 11.将json串中的属性值转换成枚举
     *
     * @JsonFormat
     * @JsonIgnore
     * @JsonIgnoreProperties
     * @JsonIgnoreType
     * @JsonProperty
     * @JsonRootName
     */
    @Test
    public void test1() {

    }
}
