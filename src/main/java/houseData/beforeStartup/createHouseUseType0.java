package houseData.beforeStartup;

import com.bean.HouseUseType;
import com.mapper.HouseUseTypeMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * house DESIGN_USE_TYPE 分组 统计 用途 添加房屋的USETYPE HOUSETYPE  DESIGN_USE_TYPE=其它的 通过houseOrder ，floorName 判断
 * 对应的记录 写在INTEGRATION 的use_type，other_house_type
 */
public class createHouseUseType0 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement;
    private static ResultSet resultSet;

    public static void main(String agr[]) throws SQLException {
        statement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        HouseUseTypeMapper houseUseTypeMapper = sqlSession.getMapper(HouseUseTypeMapper.class);
        HouseUseType houseUseType = null;
        try{
            //List<HouseUseType> houseUseTypeList = houseUseTypeMapper.findAll();
            Map<String,Object> map = new HashMap<>();
            resultSet = statement.executeQuery("select DESIGN_USE_TYPE from HOUSE_INFO.HOUSE group by HOUSE.DESIGN_USE_TYPE order by length(HOUSE.DESIGN_USE_TYPE)");
            resultSet.last();
            int i=0,sumCount = resultSet.getRow();
            resultSet.beforeFirst();

            while (resultSet.next()){
                houseUseType = houseUseTypeMapper.selectByDesignUseType(resultSet.getString("DESIGN_USE_TYPE"));

                if (houseUseType == null){
                    map.put("designUseType",resultSet.getString("DESIGN_USE_TYPE"));
                    houseUseTypeMapper.addHouseUseType(map);
                    System.out.println("add 记录 houseUseType--"+resultSet.getString("DESIGN_USE_TYPE"));
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
