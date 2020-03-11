package com.john.util;

import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {
    public static JSONObject getJSONObject(String data){
        try{
            return JSONObject.parseObject(data);
        }catch (Exception e){
            return null;
        }
    }
}
