package edu.hust.soict.bigdata.facilities.platform.http;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HttpCaller {
    private static final Logger logger = LoggerFactory.getLogger(HttpCaller.class);
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String DELETE_METHOD = "DELETE";
    private static final String PUT_METHOD = "PUT";

    public String get(String api, Map<String, String> params, Map<String, String> headers, int timeout)
            throws IOException {
        api += "?" + getParamsString(params);
        URL url = new URL(api);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod(GET_METHOD);
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
        connection.setDoOutput(true);

        if (headers != null) {
            headers.forEach(connection::setRequestProperty);
        }

        return getResponse(connection);
    }

    public String post(String api, Map<String, String> params, Map<String, String> headers, int timeout) throws IOException {
        URL url = new URL(api);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(POST_METHOD);
        conn.setConnectTimeout(timeout);
        conn.setReadTimeout(timeout);
        conn.setDoOutput(true);

        try(DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
            out.writeBytes(getParamsString(params));
            out.flush();
        }

        if (headers != null) {
            headers.forEach(conn::setRequestProperty);
        }

        return getResponse(conn);
    }

    public String post(String api, JSONObject request_body, Map<String, String> header, int timeout) throws IOException {
        URL url = new URL(api);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod(POST_METHOD);
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json;charset=utf-8");
        connection.setRequestProperty("Accept", "application/json");

        // header
        if (header != null && header.size() > 0) {
            header.forEach(connection::setRequestProperty);
        }

        // create body
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = request_body.toString().trim().getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        return getResponse(connection);
    }

    public String put(String api, JSONObject request_body, Map<String, String> header, int timeout) throws IOException {
        URL url = new URL(api);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod(PUT_METHOD);
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json;charset=utf-8");
        connection.setRequestProperty("Accept", "application/json");

        // header
        if (header != null && header.size() > 0) {
            header.forEach(connection::setRequestProperty);
        }

        // create body
        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = request_body.toString().trim().getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        return getResponse(connection);
    }

    public String delete(String api, Map<String, String> queryParams, Map<String, String> headerParams,
                         int timeout) throws IOException {
        api += "?" + getParamsString(queryParams);
        URL url = new URL(api);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod(DELETE_METHOD);
        conn.setConnectTimeout(timeout);
        conn.setReadTimeout(timeout);
        conn.setDoOutput(true);
        if (headerParams != null) headerParams.forEach(conn::setRequestProperty);

        return getResponse(conn);
    }

    private String getParamsString(Map<String, String> params)
            throws UnsupportedEncodingException {
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
            result.append("&");
        }

        String resultString = result.toString();
        return resultString.length() > 0
                ? resultString.substring(0, resultString.length() - 1)
                : resultString;
    }

    private String getResponse(HttpURLConnection connection) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            StringBuilder lineBuilder = new StringBuilder();
            while ((line = in.readLine()) != null) {
                lineBuilder.append(line);
            }
            return lineBuilder.toString();
        } finally {
            connection.disconnect();
        }
    }
}
