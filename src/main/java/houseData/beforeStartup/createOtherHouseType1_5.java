package houseData.beforeStartup;

import com.bean.OtherHouseType;
import com.mapper.OtherHouseTypeMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createOtherHouseType1_5 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;
    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OtherHouseTypeMapper otherHouseTypeMapper = sqlSession.getMapper(OtherHouseTypeMapper.class);
        OtherHouseType otherHouseType = null;
        try{
            Map<String,Object> map = new HashMap<>();
            /**
             *
             select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它'
             and HOUSE_ORDER not like '%车库%' and HOUSE_ORDER not like '%阁楼%'
             and HOUSE_ORDER not like '%物业%' and HOUSE_ORDER not like '库房%'
             and HOUSE_ORDER not like '%社区%' AND HOUSE_ORDER not like '%门卫%'
             AND HOUSE_ORDER not like '%活动%' AND HOUSE_ORDER not like '%保安室%'
             AND HOUSE_ORDER not like '%值班室%' AND HOUSE_ORDER NOT like '%公厕%'
             AND HOUSE_ORDER not like '%商场%'
             AND HOUSE_ORDER not like '%专卖店%' AND  HOUSE_ORDER not like '%会所%'
             AND HOUSE_ORDER not like '%商业银行%' AND  HOUSE_ORDER not like '%超市%'
             AND HOUSE_ORDER not like '门市%'
             AND HOUSE_ORDER not like '%一层厦子%'
             AND HOUSE_ORDER not like '%一层库房%' AND  HOUSE_ORDER not like '%仓储%'
             AND HOUSE_ORDER not like '%仓库%' AND  HOUSE_ORDER not like '%仓库及库房%'
             AND HOUSE_ORDER not like '储藏%' AND  HOUSE_ORDER not like '%厦子%'
             AND HOUSE_ORDER not like '%垃圾站%'
             AND HOUSE_ORDER not like '%换热站%' AND  HOUSE_ORDER not like '%消防控制室%'
             AND HOUSE_ORDER not like '%消防通道%' AND  HOUSE_ORDER not like '%监控室%'
             AND HOUSE_ORDER not like '通道口%' AND  HOUSE_ORDER not like '%锅炉房%'
             AND HOUSE_ORDER not like '%杂物间%'
             AND HOUSE_ORDER not like '%楼梯间%' and HOUSE_ORDER not like '%夏子%'
             AND HOUSE_ORDER not like '%售楼处%' and HOUSE_ORDER not like '%门洞%'
             and HOUSE_ORDER  not like '%入户大堂%'
             and HOUSE_ORDER <> '一层'
             and HOUSE_ORDER not like '%无%' and HOUSE_ORDER not like '%无房%' and  HOUSE_ORDER not like '%bg%' and  HOUSE_ORDER not like '%sz%'
             and HOUSE_ORDER not like '%重号%' and HOUSE_ORDER not like '%无房%' and  HOUSE_ORDER  not like '%bg%' and  HOUSE_ORDER not like '%sz%'
             and HOUSE_ORDER <> '养老中心'
             and HOUSE_ORDER not like '%地下层%' and HOUSE_ORDER not like '%居家养老%'
             and HOUSE_ORDER not like '%养%'
             and HOUSE_ORDER <> '101消控室' and HOUSE_ORDER not like '%变电%' and HOUSE_ORDER not like '%泵房%' and HOUSE_ORDER not like '%楼梯%'
             and HOUSE_ORDER not like '%泵房%' and HOUSE_ORDER not like '%楼梯%'
             and HOUSE_ORDER not like '%守卫%' and HOUSE_ORDER not like '%办公%'
             and HOUSE_ORDER not like '%网点%' and HOUSE_ORDER not like '%闷顶%'
             group by HOUSE_ORDER;

             */
            //1 车库跑出来
            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' and  HOUSE_ORDER like '闷顶%' order by length(ID)");

//            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' and (HOUSE_ORDER like '%守卫%' " +
//                    "or HOUSE_ORDER like '%办公%') order by length(ID)");












            resultSet.last();
            int i=0,sumCount = resultSet.getRow();
            resultSet.beforeFirst();

            while (resultSet.next()){
                otherHouseType = otherHouseTypeMapper.selectByHouseId(resultSet.getString("ID"));
                if (otherHouseType == null){
                    map.put("houseId",resultSet.getString("ID"));
                    map.put("houseType","OFFICE");
                    otherHouseTypeMapper.addOtherHouseType(map);
                    System.out.println("add 房屋类型记录--"+resultSet.getString("ID"));
                    sqlSession.commit();
                }
                i++;
                System.out.println("Count --"+sumCount+"/"+i);
            }



        }catch (Exception e){
            e.printStackTrace();
            return;
        }finally {
            sqlSession.close();
            if(resultSet!=null){
                resultSet.close();
            }
            if(statement!=null){
                statement.close();
            }
            MyConnection.closeConnection();


        }
    }
}
