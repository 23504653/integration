package houseData.recordBuild;

import com.bean.OwnerRecordHouseId;
import com.bean.OwnerRecordProjectId;
import com.mapper.OwnerRecordHouseIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.mapper.ProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import com.utils.Q;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class addDefin {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static String MDB_URL = "jdbc:mariadb://localhost:3310/record_building?useUnicode=true&useSSL=false&characterEncoding=utf8&serverTimezone=GMT%2B8";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/addDefine40.sql";
    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement mackCardStatement;
    private static ResultSet mackCardResultSet;

    private static Statement moreCardStatement;
    private static ResultSet moreCardResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    public static void main(String agr[]) throws SQLException {

//删除项目备案业务
        projectBusinessFile = new File(PROJECT_FILE);
        if(projectBusinessFile.exists()){
            projectBusinessFile.delete();
        }

        try{
            projectBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(projectBusinessFile.getAbsoluteFile());
            projectBusinessWriter = new BufferedWriter(fw);
            projectBusinessWriter.write("USE record_building;");
            projectBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("projectBusinessWriter 文件创建失败");
            e.printStackTrace();
            return;
        }
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);// HOUSE_OWNER_RECORD
        mackCardStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        moreCardStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getMDBStatement(MDB_URL,USER,PASSWORD);//mariadb record_building


//        SqlSession sqlSession = MybatisUtils.getSqlSession();
//        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
//        OwnerRecordHouseId ownerRecordHouseId=null;

        try {
            //有初始登记业务 没有权证信息
            buildResultSet = buildStatement.executeQuery("select hs.house_id,hs.house_info_id from " +
                    "house_register_business as hrb left join house_business as hb on " +
                    "    hrb.business_id = hb.business_id left join house_snapshot as hs on hb.info_id " +
                    "    = hs.house_info_id left join house_register_snapshot as hrs on hs.register_info_id " +
                    "where hb.work_type='BUSINESS' and hrs.register_info_id is null");


            buildResultSet.last();
            int sumCount = buildResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            buildResultSet.beforeFirst();
            while (buildResultSet.next()){



                workbookResultSet = workbookStatement.executeQuery("select rh.id,H.id as houseBId,bh.HOUSE_CODE,bh.BUSINESS_ID,o.RECORD_TIME from OWNER_BUSINESS as o " +
                        "left join BUSINESS_HOUSE as bh on o.ID=bh.BUSINESS_ID " +
                        "left join HOUSE AS H ON bh.AFTER_HOUSE=H.ID " +
                        "Left join INTEGRATION.ownerRecordHouseId as rh on H.ID=rh.oid " +
                        "WHERE o.DEFINE_ID = 'WP40' and o.DEFINE_NAME='项目备案' and o.STATUS='COMPLETE' and rh.id="+buildResultSet.getLong("house_info_id")+";");


                if(workbookResultSet.next()){
                    System.out.println( "1111----"+ workbookResultSet.getString("houseBId")+"-----"+workbookResultSet.getString("BUSINESS_ID"));
                    mackCardResultSet = mackCardStatement.executeQuery("select * from MAKE_CARD where BUSINESS_ID='"+workbookResultSet.getString("BUSINESS_ID")+"';");
                    if(mackCardResultSet.next()){

                        mackCardResultSet.last();
                        int mackCardCount=mackCardResultSet.getRow();
                        if(mackCardCount==1){
                            System.out.println( "222--housecode--"+ workbookResultSet.getString("HOUSE_CODE")+"-----"+workbookResultSet.getString("BUSINESS_ID"));

                            projectBusinessWriter.newLine();
                            projectBusinessWriter.write("INSERT house_register_snapshot (register_info_id, register_number, register_date, register_gov, register_type)  value ");
                            projectBusinessWriter.write("(" + Q.v(Long.toString(buildResultSet.getLong("house_info_id")),Q.pm(mackCardResultSet.getString("NUMBER"))
                                    ,Q.pm(workbookResultSet.getTimestamp("RECORD_TIME")),Q.pm("东港市房地产管理处")
                                    ,Q.pm("HOUSE")
                            )+ ");");

                            projectBusinessWriter.newLine();
                            projectBusinessWriter.write("update record_building.house_snapshot set register_info_id="+buildResultSet.getLong("house_info_id")+" where house_info_id="+buildResultSet.getLong("house_info_id")+";");


                        }
                        if(mackCardCount>1){
                            System.out.println("33333");
                            moreCardResultSet = moreCardStatement.executeQuery("select mc.* from BUSINESS_HOUSE as bh left join HOUSE_OWNER AS ho on bh.AFTER_HOUSE =ho.HOUSE " +
                                    "left join POWER_OWNER as po on ho.POOL=po.ID " +
                                    "left join MAKE_CARD AS mc on po.CARD = mc.ID " +
                                    "where po.CARD is not null and po.TYPE='INIT' and bh.HOUSE_CODE = '"+workbookResultSet.getString("HOUSE_CODE")+"' and bh.BUSINESS_ID='"+workbookResultSet.getString("BUSINESS_ID")+"'");
                            if(moreCardResultSet.next()){



                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT house_register_snapshot (register_info_id, register_number, register_date, register_gov, register_type)  value ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(buildResultSet.getLong("house_info_id")),Q.pm(moreCardResultSet.getString("NUMBER"))
                                        ,Q.pm(workbookResultSet.getTimestamp("RECORD_TIME")),Q.pm("东港市房地产管理处")
                                        ,Q.pm("HOUSE")
                                )+ ");");

                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("update record_building.house_snapshot set register_info_id="+buildResultSet.getLong("house_info_id")+" where house_info_id="+buildResultSet.getLong("house_info_id")+";");


                            }

                        }
                    }

                    projectBusinessWriter.flush();
                }

//
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));

            }

        }catch (Exception e){
            System.out.println("id is errer-----house_io:"+buildResultSet.getString("house_id"));
            e.printStackTrace();

        }finally {

            if(mackCardResultSet!=null){
                mackCardResultSet.close();
            }
            if(moreCardResultSet!=null){
                moreCardResultSet.close();
            }
            if(workbookResultSet!=null){
                workbookResultSet.close();
            }

            if(buildResultSet!=null){
                buildResultSet.close();
            }

            if(mackCardStatement!=null){
                mackCardStatement.close();
            }
            if(moreCardStatement!=null){
                moreCardStatement.close();
            }

            if(workbookStatement!=null){
                workbookStatement.close();
            }
            if(buildStatement!=null){
                buildStatement.close();
            }


        }



    }

}
