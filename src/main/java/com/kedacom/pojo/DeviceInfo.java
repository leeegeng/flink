package com.kedacom.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeviceInfo {
    private String name;
    private int status;
    private int carNum; // 过车数量
    private long time;

}
