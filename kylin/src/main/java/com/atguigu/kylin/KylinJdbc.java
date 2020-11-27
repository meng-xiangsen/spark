package com.atguigu.kylin;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class KylinJdbc {
    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.kylin.jdbc.Driver");

        Connection connection = DriverManager.getConnection("jdbc:kylin://hadoop102:7070/gmall", "ADMIN", "KYLIN");
        String str = "select DWD_DIM_BASE_PROVINCE.PROVINCE_NAME,DWD_DIM_BASE_PROVINCE.REGION_NAME,DWD_DIM_ACTIVITY_INFO_VIEW.ACTIVITY_NAME \n" +
                "from DWD_FACT_ORDER_INFO left join DWD_DIM_BASE_PROVINCE \n" +
                "on DWD_FACT_ORDER_INFO.PROVINCE_ID =DWD_DIM_BASE_PROVINCE.ID \n" +
                "left join DWD_DIM_ACTIVITY_INFO_VIEW on\n" +
                "DWD_DIM_ACTIVITY_INFO_VIEW.ID =DWD_FACT_ORDER_INFO.ACTIVITY_ID  ";
        PreparedStatement ps = connection.prepareStatement(str);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString(1) + ","
                    + resultSet.getString(2) + "," + resultSet.getString(3));
        }
resultSet.close();
        ps.close();
        connection.close();

    }
}
