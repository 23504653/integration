package com.mapper;

import com.bean.PowerOwnerId;

import java.util.Map;

public interface PowerOwnerIdMapper {
    PowerOwnerId selectByOldId(String OldId);
    int addPowerOwnerId(Map<String,Object> map);
    Integer findMaxId();
}
