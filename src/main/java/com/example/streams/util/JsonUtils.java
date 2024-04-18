package com.example.streams.util;

import com.jayway.jsonpath.JsonPath;

public class JsonUtils {
    public static Long getProductId(String jsonString) {
        return JsonPath.parse(jsonString).read("$.productId", Long.class);
    }

    public static Long getAmount(String jsonString) {
        return JsonPath.parse(jsonString).read("$.amount", Long.class);
    }

    public static String getSendingJson(Long productId, long amount) {
        String jsonData = "{\"productId\":%d, \"windowedAmount\":%d,\"createdAt\"%d}";
        return String.format(jsonData, productId, amount, System.currentTimeMillis());
    }
}
