package com.mapper;

import com.bean.ContractBusinessPoolId;

import java.util.Map;

public interface ContractBusinessPoolIdMapper {

    ContractBusinessPoolId selectByOldId(String OId);
    int addId(Map<String,Object> map);
    Integer findMaxId();
}
