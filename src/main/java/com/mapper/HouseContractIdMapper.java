package com.mapper;

import com.bean.HouseContractId;

import java.util.Map;

public interface HouseContractIdMapper {

    HouseContractId selectByOldId(String Oid);
    int addHouseContractId(Map<String,Object> map);
    Integer findMaxId();
}
