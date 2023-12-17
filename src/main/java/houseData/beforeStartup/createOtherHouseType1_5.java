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
             select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它'
             select HOUSE_ORDER from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' group by HOUSE_ORDER;
             select IN_FLOOR_NAME from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' group by IN_FLOOR_NAME;
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
             and IN_FLOOR_NAME<>'1-2层网点'
             and IN_FLOOR_NAME<>'1F 网点' and IN_FLOOR_NAME<>'1F　网点' and IN_FLOOR_NAME<>'1F网点'
             and IN_FLOOR_NAME<>'1F门市'  and IN_FLOOR_NAME<>'1、2层商业网点' and  IN_FLOOR_NAME<>'1层网点'
             and IN_FLOOR_NAME <> '2F 网点' and IN_FLOOR_NAME <> '2F网点' and IN_FLOOR_NAME<>'二层门市'
             and IN_FLOOR_NAME <> '1层　网点'
             and IN_FLOOR_NAME<>'1F  车库'
             and IN_FLOOR_NAME<>'1F 车库' and IN_FLOOR_NAME<>'1F　车库' and IN_FLOOR_NAME<>'1F车库'
             and IN_FLOOR_NAME<>'1层北侧车库'  and IN_FLOOR_NAME<>'1层车库' and  IN_FLOOR_NAME<>'车库'
             and IN_FLOOR_NAME<>'1F 库房' and IN_FLOOR_NAME<>'一层厦子' and IN_FLOOR_NAME<>'储藏室'
             and IN_FLOOR_NAME<>'1层库房' and IN_FLOOR_NAME<>'库房' and IN_FLOOR_NAME<>'厦子'
             and IN_FLOOR_NAME<>'1F厦子' and IN_FLOOR_NAME<>'1F　藏储'
             and IN_FLOOR_NAME<>'6F 阁楼'
             and IN_FLOOR_NAME<>'6层阁楼' and IN_FLOOR_NAME<>'阁楼' and IN_FLOOR_NAME<>'阁楼层'
             and IN_FLOOR_NAME<>'1F 车库　库房'
             and IN_FLOOR_NAME<>'1F　车库　库房' and IN_FLOOR_NAME<>'1F车库-仓库' and IN_FLOOR_NAME<>'1F车库及储藏室'
             and IN_FLOOR_NAME<>'1F车库及库房' and IN_FLOOR_NAME<>'1F车库库房' and IN_FLOOR_NAME<>'1层　库房　车房'
             and IN_FLOOR_NAME<>'1层　自行车库' and IN_FLOOR_NAME<>'1层自行车库' and IN_FLOOR_NAME<>'1层车库　储藏室'
             and IN_FLOOR_NAME<>'1层车库　库房' and IN_FLOOR_NAME<>'1层车库及库房' and IN_FLOOR_NAME<>'1层车库库房'
             and IN_FLOOR_NAME<>'车库及储藏室' and IN_FLOOR_NAME<>'车库及储藏层'
             and IN_FLOOR_NAME<>'1F-2F车库 网点'
             and IN_FLOOR_NAME<>'1F 车库库房网点' and IN_FLOOR_NAME<>'1F-2F车库 网点' and IN_FLOOR_NAME<>'1F-2F车库网点'
             and IN_FLOOR_NAME<>'1F　网点　车库' and IN_FLOOR_NAME<>'1层车库　储藏室　网点' and IN_FLOOR_NAME<>'1层　库房　车房'
             and IN_FLOOR_NAME<>'网点-车库' and IN_FLOOR_NAME<>'1F　藏储　网点' and IN_FLOOR_NAME<>'厦子网点'
             and IN_FLOOR_NAME<>'1层住宅及网点'  and IN_FLOOR_NAME<> '1层住宅网点'
             and IN_FLOOR_NAME<>'1层商场　网点　车库' and IN_FLOOR_NAME<>'1层商场　网点　车库' and IN_FLOOR_NAME<>'1层网点　商场　车库'
             and IN_FLOOR_NAME<>'1层网点库房' and IN_FLOOR_NAME<>'2F    网点  住宅' and IN_FLOOR_NAME<>'2F 网点住宅'
             and IN_FLOOR_NAME<>'2F 网点及住宅'  and IN_FLOOR_NAME<>'2层　住宅　网点' and IN_FLOOR_NAME<>'2F 网点住宅'
             and ID not IN(SELECT houseId FROM INTEGRATION.other_house_type)
             group by IN_FLOOR_NAME;

             select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' and (IN_FLOOR_NAME='1层住宅及网点'
             or IN_FLOOR_NAME='1层商场　网点　车库' OR IN_FLOOR_NAME='1层商场　网点　车库' OR IN_FLOOR_NAME='1层网点　商场　车库'
             OR IN_FLOOR_NAME='1层网点库房' OR IN_FLOOR_NAME='2F    网点  住宅' or IN_FLOOR_NAME='2F 网点住宅'
             OR IN_FLOOR_NAME='2F 网点及住宅'  OR IN_FLOOR_NAME='2层　住宅　网点' or IN_FLOOR_NAME='2F 网点住宅'
             ) order by length(ID);
             */
            //1 车库跑出来
//            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' and  HOUSE_ORDER like '闷顶%' order by length(ID)");

//            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' and (IN_FLOOR_NAME='1-2层网点' " +
//                    "or IN_FLOOR_NAME='1F 网点' OR IN_FLOOR_NAME='1F　网点' OR IN_FLOOR_NAME='1F网点' OR IN_FLOOR_NAME='1层　网点'  OR IN_FLOOR_NAME='1、2层商业网点' OR  IN_FLOOR_NAME='1层网点' "+
//                    "or IN_FLOOR_NAME = '2F 网点' OR IN_FLOOR_NAME = '2F网点' or IN_FLOOR_NAME='二层门市') order by length(ID)");


//            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它' " +
//                    "and HOUSE_ORDER not like '%车库%' and HOUSE_ORDER not like '%阁楼%' " +
//                    "and HOUSE_ORDER not like '%物业%' and HOUSE_ORDER not like '库房%' " +
//                    "and HOUSE_ORDER not like '%社区%' AND HOUSE_ORDER not like '%门卫%' " +
//                    "AND HOUSE_ORDER not like '%活动%' AND HOUSE_ORDER not like '%保安室%' " +
//                    "AND HOUSE_ORDER not like '%值班室%' AND HOUSE_ORDER NOT like '%公厕%' " +
//                    "AND HOUSE_ORDER not like '%商场%' " +
//                    "AND HOUSE_ORDER not like '%专卖店%' AND  HOUSE_ORDER not like '%会所%' " +
//                    "AND HOUSE_ORDER not like '%商业银行%' AND  HOUSE_ORDER not like '%超市%' " +
//                    "AND HOUSE_ORDER not like '门市%' " +
//                    "AND HOUSE_ORDER not like '%一层厦子%' " +
//                    "AND HOUSE_ORDER not like '%一层库房%' AND  HOUSE_ORDER not like '%仓储%' " +
//                    "AND HOUSE_ORDER not like '%仓库%' AND  HOUSE_ORDER not like '%仓库及库房%' " +
//                    "AND HOUSE_ORDER not like '储藏%' AND  HOUSE_ORDER not like '%厦子%' " +
//                    "AND HOUSE_ORDER not like '%垃圾站%' " +
//                    "AND HOUSE_ORDER not like '%换热站%' AND  HOUSE_ORDER not like '%消防控制室%' " +
//                    "AND HOUSE_ORDER not like '%消防通道%' AND  HOUSE_ORDER not like '%监控室%' " +
//                    "AND HOUSE_ORDER not like '通道口%' AND  HOUSE_ORDER not like '%锅炉房%' " +
//                    "AND HOUSE_ORDER not like '%杂物间%' " +
//                    "AND HOUSE_ORDER not like '%楼梯间%' and HOUSE_ORDER not like '%夏子%' " +
//                    "AND HOUSE_ORDER not like '%售楼处%' and HOUSE_ORDER not like '%门洞%' " +
//                    "and HOUSE_ORDER  not like '%入户大堂%' " +
//                    "and HOUSE_ORDER <> '一层' " +
//                    "and HOUSE_ORDER not like '%无%' and HOUSE_ORDER not like '%无房%' and  HOUSE_ORDER not like '%bg%' and  HOUSE_ORDER not like '%sz%' " +
//                    "and HOUSE_ORDER not like '%重号%' and HOUSE_ORDER not like '%无房%' and  HOUSE_ORDER  not like '%bg%' and  HOUSE_ORDER not like '%sz%' " +
//                    "and HOUSE_ORDER <> '养老中心' " +
//                    "and HOUSE_ORDER not like '%地下层%' and HOUSE_ORDER not like '%居家养老%' " +
//                    "and HOUSE_ORDER not like '%养%' " +
//                    "and HOUSE_ORDER <> '101消控室' and HOUSE_ORDER not like '%变电%' and HOUSE_ORDER not like '%泵房%' and HOUSE_ORDER not like '%楼梯%' " +
//                    "and HOUSE_ORDER not like '%泵房%' and HOUSE_ORDER not like '%楼梯%' " +
//                    "and HOUSE_ORDER not like '%守卫%' and HOUSE_ORDER not like '%办公%' " +
//                    "and HOUSE_ORDER not like '%网点%' and HOUSE_ORDER not like '%闷顶%' " +
//                    "and IN_FLOOR_NAME<>'1-2层网点' " +
//                    "and IN_FLOOR_NAME<>'1F 网点' and IN_FLOOR_NAME<>'1F　网点' and IN_FLOOR_NAME<>'1F网点' " +
//                    "and IN_FLOOR_NAME<>'1F门市'  and IN_FLOOR_NAME<>'1、2层商业网点' and  IN_FLOOR_NAME<>'1层网点' " +
//                    "and IN_FLOOR_NAME <> '2F 网点' and IN_FLOOR_NAME <> '2F网点' and IN_FLOOR_NAME<>'二层门市' " +
//                    "and IN_FLOOR_NAME <> '1层　网点' " +
//                    "and IN_FLOOR_NAME<>'1F  车库' " +
//                    "and IN_FLOOR_NAME<>'1F 车库' and IN_FLOOR_NAME<>'1F　车库' and IN_FLOOR_NAME<>'1F车库' " +
//                    "and IN_FLOOR_NAME<>'1层北侧车库'  and IN_FLOOR_NAME<>'1层车库' and  IN_FLOOR_NAME<>'车库' " +
//                    "and IN_FLOOR_NAME<>'1F 库房' and IN_FLOOR_NAME<>'一层厦子' and IN_FLOOR_NAME<>'储藏室' " +
//                    "and IN_FLOOR_NAME<>'1层库房' and IN_FLOOR_NAME<>'库房' and IN_FLOOR_NAME<>'厦子' " +
//                    "and IN_FLOOR_NAME<>'1F厦子' and IN_FLOOR_NAME<>'1F　藏储' " +
//                    "and IN_FLOOR_NAME<>'6F 阁楼' " +
//                    "and IN_FLOOR_NAME<>'6层阁楼' and IN_FLOOR_NAME<>'阁楼' and IN_FLOOR_NAME<>'阁楼层' " +
//                    "and IN_FLOOR_NAME<>'1F 车库　库房' " +
//                    "and IN_FLOOR_NAME<>'1F　车库　库房' and IN_FLOOR_NAME<>'1F车库-仓库' and IN_FLOOR_NAME<>'1F车库及储藏室' " +
//                    "and IN_FLOOR_NAME<>'1F车库及库房' and IN_FLOOR_NAME<>'1F车库库房' and IN_FLOOR_NAME<>'1层　库房　车房' " +
//                    "and IN_FLOOR_NAME<>'1层　自行车库' and IN_FLOOR_NAME<>'1层自行车库' and IN_FLOOR_NAME<>'1层车库　储藏室' " +
//                    "and IN_FLOOR_NAME<>'1层车库　库房' and IN_FLOOR_NAME<>'1层车库及库房' and IN_FLOOR_NAME<>'1层车库库房' " +
//                    "and IN_FLOOR_NAME<>'车库及储藏室' and IN_FLOOR_NAME<>'车库及储藏层' " +
//                    "and IN_FLOOR_NAME<>'1F-2F车库 网点' " +
//                    "and IN_FLOOR_NAME<>'1F 车库库房网点' and IN_FLOOR_NAME<>'1F-2F车库 网点' and IN_FLOOR_NAME<>'1F-2F车库网点' " +
//                    "and IN_FLOOR_NAME<>'1F　网点　车库' and IN_FLOOR_NAME<>'1层车库　储藏室　网点' and IN_FLOOR_NAME<>'1层　库房　车房' " +
//                    "and IN_FLOOR_NAME<>'网点-车库' and IN_FLOOR_NAME<>'1F　藏储　网点' and IN_FLOOR_NAME<>'厦子网点' " +
//                    "and IN_FLOOR_NAME<>'1层住宅及网点'  and IN_FLOOR_NAME<> '1层住宅网点' " +
//                    "and IN_FLOOR_NAME<>'1层商场　网点　车库' and IN_FLOOR_NAME<>'1层商场　网点　车库' and IN_FLOOR_NAME<>'1层网点　商场　车库' " +
//                    "and IN_FLOOR_NAME<>'1层网点库房' and IN_FLOOR_NAME<>'2F    网点  住宅' and IN_FLOOR_NAME<>'2F 网点住宅' " +
//                    "and IN_FLOOR_NAME<>'2F 网点及住宅'  and IN_FLOOR_NAME<>'2层　住宅　网点' and IN_FLOOR_NAME<>'2F 网点住宅' " +
//                    "and ID not IN(SELECT houseId FROM INTEGRATION.other_house_type) " +
//                    "group by IN_FLOOR_NAME;");





//            select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它'
//            and HOUSE_ORDER not like '%车库%' and HOUSE_ORDER not like '%阁楼%'
//            and HOUSE_ORDER not like '%物业%' and HOUSE_ORDER not like '库房%'
//            and HOUSE_ORDER not like '%社区%' AND HOUSE_ORDER not like '%门卫%'
//            AND HOUSE_ORDER not like '%活动%' AND HOUSE_ORDER not like '%保安室%'
//            AND HOUSE_ORDER not like '%值班室%' AND HOUSE_ORDER NOT like '%公厕%'
//            AND HOUSE_ORDER not like '%商场%'
//            AND HOUSE_ORDER not like '%专卖店%' AND  HOUSE_ORDER not like '%会所%'
//            AND HOUSE_ORDER not like '%商业银行%' AND  HOUSE_ORDER not like '%超市%'
//            AND HOUSE_ORDER not like '门市%'
//            AND HOUSE_ORDER not like '%一层厦子%'
//            AND HOUSE_ORDER not like '%一层库房%' AND  HOUSE_ORDER not like '%仓储%'
//            AND HOUSE_ORDER not like '%仓库%' AND  HOUSE_ORDER not like '%仓库及库房%'
//            AND HOUSE_ORDER not like '储藏%' AND  HOUSE_ORDER not like '%厦子%'
//            AND HOUSE_ORDER not like '%垃圾站%'
//            AND HOUSE_ORDER not like '%换热站%' AND  HOUSE_ORDER not like '%消防控制室%'
//            AND HOUSE_ORDER not like '%消防通道%' AND  HOUSE_ORDER not like '%监控室%'
//            AND HOUSE_ORDER not like '通道口%' AND  HOUSE_ORDER not like '%锅炉房%'
//            AND HOUSE_ORDER not like '%杂物间%'
//            AND HOUSE_ORDER not like '%楼梯间%' and HOUSE_ORDER not like '%夏子%'
//            AND HOUSE_ORDER not like '%售楼处%' and HOUSE_ORDER not like '%门洞%'
//            and HOUSE_ORDER  not like '%入户大堂%'
//            and HOUSE_ORDER <> '一层'
//            and HOUSE_ORDER not like '%无%' and HOUSE_ORDER not like '%无房%' and  HOUSE_ORDER not like '%bg%' and  HOUSE_ORDER not like '%sz%'
//            and HOUSE_ORDER not like '%重号%' and HOUSE_ORDER not like '%无房%' and  HOUSE_ORDER  not like '%bg%' and  HOUSE_ORDER not like '%sz%'
//            and HOUSE_ORDER <> '养老中心'
//            and HOUSE_ORDER not like '%地下层%' and HOUSE_ORDER not like '%居家养老%'
//            and HOUSE_ORDER not like '%养%'
//            and HOUSE_ORDER <> '101消控室' and HOUSE_ORDER not like '%变电%' and HOUSE_ORDER not like '%泵房%' and HOUSE_ORDER not like '%楼梯%'
//            and HOUSE_ORDER not like '%泵房%' and HOUSE_ORDER not like '%楼梯%'
//            and HOUSE_ORDER not like '%守卫%' and HOUSE_ORDER not like '%办公%'
//            and HOUSE_ORDER not like '%网点%' and HOUSE_ORDER not like '%闷顶%'
//            and IN_FLOOR_NAME<>'1-2层网点'
//            and IN_FLOOR_NAME<>'1F 网点' and IN_FLOOR_NAME<>'1F　网点' and IN_FLOOR_NAME<>'1F网点'
//            and IN_FLOOR_NAME<>'1F门市'  and IN_FLOOR_NAME<>'1、2层商业网点' and  IN_FLOOR_NAME<>'1层网点'
//            and IN_FLOOR_NAME <> '2F 网点' and IN_FLOOR_NAME <> '2F网点' and IN_FLOOR_NAME<>'二层门市'
//            and IN_FLOOR_NAME <> '1层　网点'
//            and IN_FLOOR_NAME<>'1F  车库'
//            and IN_FLOOR_NAME<>'1F 车库' and IN_FLOOR_NAME<>'1F　车库' and IN_FLOOR_NAME<>'1F车库'
//            and IN_FLOOR_NAME<>'1层北侧车库'  and IN_FLOOR_NAME<>'1层车库' and  IN_FLOOR_NAME<>'车库'
//            and IN_FLOOR_NAME<>'1F 库房' and IN_FLOOR_NAME<>'一层厦子' and IN_FLOOR_NAME<>'储藏室'
//            and IN_FLOOR_NAME<>'1层库房' and IN_FLOOR_NAME<>'库房' and IN_FLOOR_NAME<>'厦子'
//            and IN_FLOOR_NAME<>'1F厦子' and IN_FLOOR_NAME<>'1F　藏储'
//            and IN_FLOOR_NAME<>'6F 阁楼'
//            and IN_FLOOR_NAME<>'6层阁楼' and IN_FLOOR_NAME<>'阁楼' and IN_FLOOR_NAME<>'阁楼层'
//            and IN_FLOOR_NAME<>'1F 车库　库房'
//            and IN_FLOOR_NAME<>'1F　车库　库房' and IN_FLOOR_NAME<>'1F车库-仓库' and IN_FLOOR_NAME<>'1F车库及储藏室'
//            and IN_FLOOR_NAME<>'1F车库及库房' and IN_FLOOR_NAME<>'1F车库库房' and IN_FLOOR_NAME<>'1层　库房　车房'
//            and IN_FLOOR_NAME<>'1层　自行车库' and IN_FLOOR_NAME<>'1层自行车库' and IN_FLOOR_NAME<>'1层车库　储藏室'
//            and IN_FLOOR_NAME<>'1层车库　库房' and IN_FLOOR_NAME<>'1层车库及库房' and IN_FLOOR_NAME<>'1层车库库房'
//            and IN_FLOOR_NAME<>'车库及储藏室' and IN_FLOOR_NAME<>'车库及储藏层'
//            and IN_FLOOR_NAME<>'1F-2F车库 网点'
//            and IN_FLOOR_NAME<>'1F 车库库房网点' and IN_FLOOR_NAME<>'1F-2F车库 网点' and IN_FLOOR_NAME<>'1F-2F车库网点'
//            and IN_FLOOR_NAME<>'1F　网点　车库' and IN_FLOOR_NAME<>'1层车库　储藏室　网点' and IN_FLOOR_NAME<>'1层　库房　车房'
//            and IN_FLOOR_NAME<>'网点-车库' and IN_FLOOR_NAME<>'1F　藏储　网点' and IN_FLOOR_NAME<>'厦子网点'
//            and IN_FLOOR_NAME<>'1层住宅及网点'  and IN_FLOOR_NAME<> '1层住宅网点'
//            and IN_FLOOR_NAME<>'1层商场　网点　车库' and IN_FLOOR_NAME<>'1层商场　网点　车库' and IN_FLOOR_NAME<>'1层网点　商场　车库'
//            and IN_FLOOR_NAME<>'1层网点库房' and IN_FLOOR_NAME<>'2F    网点  住宅' and IN_FLOOR_NAME<>'2F 网点住宅'
//            and IN_FLOOR_NAME<>'2F 网点及住宅'  and IN_FLOOR_NAME<>'2层　住宅　网点' and IN_FLOOR_NAME<>'2F 网点住宅'
//            and ID not IN(SELECT houseId FROM INTEGRATION.other_house_type)
//            group by IN_FLOOR_NAME;








            resultSet = statement.executeQuery("select * from HOUSE_INFO.HOUSE where DESIGN_USE_TYPE='其它'");

            resultSet.last();
            int i=0,sumCount = resultSet.getRow();
            resultSet.beforeFirst();

            while (resultSet.next()){
                otherHouseType = otherHouseTypeMapper.selectByHouseId(resultSet.getString("ID"));
                if (otherHouseType == null){
                    map.put("houseId",resultSet.getString("ID"));
                    map.put("houseType","DWELLING");
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
