package com.atguigu.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF extends GenericUDTF {
    //创建数组，代表UDTF每行的输出
    private String[] output = new String[1];

    /**
     * 两个作用
     * 1 检查参数类型
     *              传入一个 一行一列：JSON数组的字符串
     * 2.声明函数输出的每行的 字段数量和类型
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //从argOIs取出传入的所有的字段
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        //检查传入的参数个数是不是1列，不是一列就抛异常
        if (inputFields.size()!=1){
            throw new UDFArgumentException("只允许传入一列string类型的字段！");
        }
        //检查传入的参数是不是String类型
        if (!"string".equals(inputFields.get(0).getFieldObjectInspector().getTypeName())){
            throw new UDFArgumentException("传入的列不是String类型!");
        }

        //2.声明函数输出的每行的 字段数量和类型
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("eventStr");

        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }
    /*
                计算
                    args[0]: [{},{}]
         */
    public void process(Object[] args) throws HiveException {
        //将传入的jsonArrayStr 转成 JsonArray对象
        JSONArray jsonArray = new JSONArray(args[0].toString());
        //遍历数组取出其中的每个JSON对象
        for (int i = 0; i <jsonArray.length() ; i++) {
            String jsonStr = jsonArray.getString(i);

            output[0] = jsonStr;
            forward(output);
        }
        
    }

    public void close() throws HiveException {

    }
}
