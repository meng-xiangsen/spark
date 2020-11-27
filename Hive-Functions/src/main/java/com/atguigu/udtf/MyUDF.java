package com.atguigu.udtf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.HashSet;

public class MyUDF extends UDF {
    public static void main(String[] args) {

    }

    public String evaluate(String a ,String b){
        if(a == null){
            return b;
        }
        if (b==null) return a;
        String[] awords = a.split("\\|");
        String[] bwords = b.split("\\|");
        HashSet<String> set = new HashSet<>(Arrays.asList(awords));
        set.addAll(Arrays.asList(bwords));
        String result = null ;
        for (String s : set) {

            result += s + "|";
        }
        return result.substring(0,result.length()-1);
    }

}
