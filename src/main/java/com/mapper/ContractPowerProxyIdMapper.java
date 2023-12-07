package com.mapper;

import com.bean.ContractPowerProxyId;

import java.util.Map;

public interface ContractPowerProxyIdMapper {

    ContractPowerProxyId selectByOldId(String OId);
    int addId(Map<String,Object> map);
    Integer findMaxId();
}
