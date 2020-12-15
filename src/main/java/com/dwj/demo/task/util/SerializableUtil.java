package com.dwj.demo.task.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author dwj
 * @date 2020/7/27 15:26
 */
public class SerializableUtil {

    //--------------------------------------------------------------
    //                  jackson 序列化/反序列化方法
    //--------------------------------------------------------------

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    /**
     * 将字节数组反序列化为原对象
     *
     * @param byteArray  writeToByteArray 方法序列化后的字节数组
     * @param targetType 原对象的 Class
     * @param <T>        原对象的类型
     * @return 原对象
     */
    public static <T> T readFromByteArray(byte[] byteArray, Class<T> targetType) {
        try {
            return OBJECT_MAPPER.readValue(byteArray, targetType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将字节数组反序列化为原对象(多态反序列化)
     *
     * @param byteArray  writeToByteArray 方法序列化后的字节数组
     * @param targetType 原对象的 Class
     * @param <T>        原对象的类型
     * @return 原对象
     */
    public static <T> T readFromByteArray(byte[] byteArray, TypeReference<T> targetType) {
        try {
            return OBJECT_MAPPER.readValue(byteArray, targetType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将对象序列化为字节数组
     *
     * @param obj 对象
     * @return 字节数组
     */
    public static byte[] objectWriteToByteArray(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将对象序列化为 json 字符串
     *
     * @param obj 对象
     * @return json
     */
    public static String obj2json(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将 json 字符串反序列化为对象
     *
     * @param json       字符串
     * @param targetType 对象类型 Class
     * @param <T>        对象类型
     * @return 对象
     */
    public static <T> T json2obj(String json, Class<T> targetType) {
        try {
            return OBJECT_MAPPER.readValue(json, targetType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"unused", "unchecked"})
    public static Map<String, Object> obj2map(Object obj) {
        if (obj == null) {
            return null;
        }
        return json2obj(obj2json(obj), HashMap.class);
    }

    public static <T> T map2obj(Map<?, ?> map, Class<T> targetType) {
        if (map == null) {
            return null;
        }
        return json2obj(obj2json(map), targetType);
    }

    @SuppressWarnings("unchecked")
    public static <T> T map2obj(Object obj, Class<T> targetType) {
        if (obj != null && (!(obj instanceof Map<?, ?>))) {
            return null;
        }
        return map2obj((Map<String, Object>) obj, targetType);
    }

}
