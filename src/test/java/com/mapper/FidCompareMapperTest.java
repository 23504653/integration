package com.mapper;

import com.bean.FidCompare;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.Test;

public class FidCompareMapperTest {
    @Test
    public void test(){
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        try{
            FidCompareMapper fidCompareMapper =  sqlSession.getMapper(FidCompareMapper.class);
            FidCompare fidCompare = fidCompareMapper.selectOne("1");
            if (fidCompare == null){
                System.out.println(11111);
            }else{
                System.out.println(222);
            }
//            System.out.println(fidCompare.toString());
//            System.out.println(fidCompare.getBusinessFileId());
//            FidCompare fidCompare2 = new FidCompare();
//            fidCompare2.setId("2");
//            fidCompare2.setBusinessFileId("BusinessFileId");
//            fidCompare2.setBusinessId("BusinessId");
//            fidCompare2.setOldFid("老ID");
//            fidCompare2.setFileName("文件名称");
//            fidCompareMapper.addFidCompare(fidCompare2);
//            sqlSession.commit();
        }catch (Exception e){
            throw new RuntimeException(e);

        }finally {
            sqlSession.close();
        }



    }
}
