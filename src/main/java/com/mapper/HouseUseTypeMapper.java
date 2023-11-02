package com.mapper;

import com.bean.HouseUseType;

import java.util.List;
import java.util.Map;

public interface HouseUseTypeMapper {
    public HouseUseType selectByDesignUseType (String designUseType);
    public int addHouseUseType(Map<String,Object> map);
    public List<HouseUseType> findAll();
}
