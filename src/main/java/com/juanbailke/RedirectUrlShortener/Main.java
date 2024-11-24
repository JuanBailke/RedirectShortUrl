package com.juanbailke.RedirectUrlShortener;

import java.io.InputStream;
import java.util.Map;
import java.util.HashMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final S3Client s3Client = S3Client.builder().build();
    private final ObjectMapper ObjectMapper = new ObjectMapper();

    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {

        String url = (String) input.get("rawPath"); //coleta a url da requisição HTTP
        String shortUrl = url.replace("/", ""); //remove o / da url

        if(shortUrl == null || shortUrl.isEmpty()) {
            throw new IllegalArgumentException("Invalid url: 'shortUrl' is required.");
        }

        //Busca o objeto no S3
        GetObjectRequest request = GetObjectRequest.builder()
        .bucket("bucket-storage-url-shortener")
        .key(shortUrl + ".json")
        .build();

        //Se o objeto não existir, retorna um erro
        InputStream s3ObjecStream; 
        try {
            s3ObjecStream = s3Client.getObject(request);
        } catch (Exception e) {
            throw new RuntimeException("Error fetching data from S3: " + e.getMessage(), e);
        }

        
        //Deserializa o objeto
        UrlData urlData;
        try {
            urlData = ObjectMapper.readValue(s3ObjecStream, UrlData.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing URL data: " + e.getMessage(), e);
        }

        //Variável para armazenar o tempo atual
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        
        Map<String, Object> response = new HashMap<>();

        //Validação de expiração da url
        if (urlData.getExpirationTime() < currentTimeSeconds) {
            response.put("statusCode", 410);
            response.put("body", "The url has expired");
            return response;
        }

        //Redirecionamento válido
        response.put("statusCode", 302);
        Map<String, String> headers = new HashMap<>();
        headers.put("Location", urlData.getOriginalUrl());
        response.put("headers", headers);

        return response;
    }
}