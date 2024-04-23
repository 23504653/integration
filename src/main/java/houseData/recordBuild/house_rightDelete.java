package houseData.recordBuild;

import com.utils.MyConnection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class house_rightDelete {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/record_building?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/house_rightDelete.sql";

    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    public static void main(String agr[]) throws SQLException {
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

        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        try {
            houseResultSet = houseStatement.executeQuery("SELECT house_id FROM house_rights where house_id=489960 GROUP BY house_id HAVING COUNT(house_id) >1;" );
            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){

                workbookResultSet=workbookStatement.executeQuery("select * from house_rights where house_id="+houseResultSet.getInt("house_id"));
                workbookResultSet.beforeFirst();
                Set<Integer> workidSet = new HashSet<>();
                Set<Integer> duplicateWorkids = new HashSet<>();
                workidSet.clear();
                duplicateWorkids.clear();
                while (workbookResultSet.next()){
                    int workid = workbookResultSet.getInt("work_id");
                    if (!workidSet.add(workid)) {
                        duplicateWorkids.add(workid);
                    }
                }
                if (!duplicateWorkids.isEmpty()) {
                    System.out.println("重复的 workid:--"+houseResultSet.getInt("house_id"));
//                    for (int wid : duplicateWorkids) {
//                        System.out.println(wid);
//                    }
                } else {
                    taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("select * from house_rights where house_id="+houseResultSet.getInt("house_id")+" and work_id not in (SELECT max(work_id) FROM house_rights where house_id="+houseResultSet.getInt("house_id")+");");
                    if(taskOperBusinessResultSet.next()){
                        taskOperBusinessResultSet.beforeFirst();
                        while (taskOperBusinessResultSet.next()){
                            System.out.println("houseid-"+taskOperBusinessResultSet.getString("house_id")+"----work_id----"+taskOperBusinessResultSet.getString("work_id"));
                            projectBusinessWriter.newLine();
                            projectBusinessWriter.write("delete from house_rights where work_id="+taskOperBusinessResultSet.getString("work_id")+";");
                            projectBusinessWriter.flush();
                        }
                    }
                    System.out.println("没有重复的 workid.");
                }




                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            e.printStackTrace();
            return;
        }finally {

        }


    }

}
