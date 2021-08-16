package com.kedacom.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeviceStatusStt {
    private String name;
    private int status;
    private long continueDurationTime;  // 持续在线/离线时长
    private int totalCarNum; // 过车数量
    private long time;  // 最新时间
}
