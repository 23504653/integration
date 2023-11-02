package com.mapper;

import com.bean.OtherHouseType;

import java.util.List;
import java.util.Map;

public interface OtherHouseTypeMapper {
    public OtherHouseType selectByHouseId(String houseId);
    public int addOtherHouseType(Map<String,Object> map);
    public List<OtherHouseType> findAll();

}
