package de.hnjansse.tuya;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tuya.open.sdk.example.MessageVO;
import com.tuya.open.sdk.mq.MqConfigs;
import com.tuya.open.sdk.mq.MqConstants;
import com.tuya.open.sdk.mq.MqConsumer;
import com.tuya.open.sdk.mq.MqEnv;
import com.tuya.open.sdk.util.decrypt.AESBaseDecryptor;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CameraEventConsumer {

    private static String accessId;
    private static String accessKey;
    private static String environment;

    private static final HttpClient httpClient =
            HttpClient.newHttpClient();

    public static void main(String[] args) throws Exception {

        accessId = System.getenv("TUYA_ACCESS_ID");
        accessKey = System.getenv("TUYA_ACCESS_SECRET");
        environment = System.getenv("TUYA_ENVIRONMENT");

        if (accessId == null || accessId.isBlank()) {
            throw new IllegalStateException("TUYA_ACCESS_ID is missing");
        }

        if (accessKey == null || accessKey.isBlank()) {
            throw new IllegalStateException("TUYA_ACCESS_SECRET is missing");
        }

        if (environment == null || environment.isBlank()) {
            environment = "test";
        }

        MqEnv mqEnv;

        if ("prod".equalsIgnoreCase(environment)) {
            mqEnv = MqEnv.PROD;
        } else {
            mqEnv = MqEnv.TEST;
        }

        System.out.println("----------------------------------------");
        System.out.println("Tuya Camera Events");
        System.out.println("Environment: " + environment);
        System.out.println("Server: EU");
        System.out.println("----------------------------------------");

        MqConsumer consumer = MqConsumer.build()
                .serviceUrl(MqConfigs.EU_SERVER_URL)
                .accessId(accessId)
                .accessKey(accessKey)
                .env(mqEnv)
                .messageListener(CameraEventConsumer::handleMessage);

        System.out.println("Connecting to Tuya Message Service...");

        consumer.start();
    }

    private static void handleMessage(Message message) {

        MessageId messageId = message.getMessageId();

        try {

            String encryptModel =
                    message.getProperty(MqConstants.ENCRYPT_MODEL);

            String payload =
                    new String(message.getData(), StandardCharsets.UTF_8);

            MessageVO messageVO =
                    JSON.parseObject(payload, MessageVO.class);

            String decrypted =
                    AESBaseDecryptor.decrypt(
                            messageVO.getData(),
                            accessKey.substring(8, 24),
                            encryptModel
                    );

            JSONObject root =
                    JSON.parseObject(decrypted);

String bizCode =
        root.getString("bizCode");

System.out.println(
        "TUYA MESSAGE RECEIVED - bizCode=" + bizCode
);

if (!"devicePropertyMessage".equals(bizCode)) {
    System.out.println(
            "TUYA MESSAGE IGNORED - unsupported bizCode"
    );
    return;
}

            JSONObject bizData =
                    root.getJSONObject("bizData");

            if (bizData == null) {
                return;
            }

            String deviceId =
                    bizData.getString("devId");

            JSONArray properties =
                    bizData.getJSONArray("properties");

            if (properties == null) {
                return;
            }

            for (int i = 0; i < properties.size(); i++) {

                JSONObject property =
                        properties.getJSONObject(i);

                String code =
                        property.getString("code");

                Integer dpId =
                        property.getInteger("dpId");

System.out.println(
        "TUYA PROPERTY - device="
                + deviceId
                + " dpId="
                + dpId
                + " code="
                + code
);

                if (!"initiative_message".equals(code)
                        || dpId == null
                        || dpId != 212) {
                    continue;
                }

                String encoded =
                        property.getString("value");

                if (encoded == null || encoded.isBlank()) {
                    continue;
                }

                String decoded =
                        new String(
                                Base64.getDecoder().decode(encoded),
                                StandardCharsets.UTF_8
                        );

                JSONObject cameraEvent =
                        JSON.parseObject(decoded);

                String command =
                        cameraEvent.getString("cmd");

                Boolean alarm =
                        cameraEvent.getBoolean("alarm");

                if ("ipc_human".equals(command)
                        && Boolean.TRUE.equals(alarm)) {

                    Long eventTime =
                            cameraEvent.getLong("time");

                    System.out.println(
                            "PERSON DETECTED - device="
                                    + deviceId
                                    + " time="
                                    + eventTime
                    );

                    fireHomeAssistantEvent(
                            deviceId,
                            eventTime
                    );
                }
            }

        } catch (Exception e) {

            System.err.println(
                    "Error processing Tuya message "
                            + messageId
            );

            e.printStackTrace();
        }
    }

    private static void fireHomeAssistantEvent(
            String deviceId,
            Long eventTime) throws Exception {

        String supervisorToken =
                System.getenv("SUPERVISOR_TOKEN");

        if (supervisorToken == null
                || supervisorToken.isBlank()) {

            throw new IllegalStateException(
                    "SUPERVISOR_TOKEN is missing"
            );
        }

        JSONObject eventData =
                new JSONObject();

        eventData.put("device_id", deviceId);
        eventData.put("command", "ipc_human");
        eventData.put("alarm", true);
        eventData.put("time", eventTime);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(
                            URI.create(
                                "http://supervisor/core/api/events/tuya_camera_human"
                            )
                        )
                        .header(
                            "Authorization",
                            "Bearer " + supervisorToken
                        )
                        .header(
                            "Content-Type",
                            "application/json"
                        )
                        .POST(
                            HttpRequest.BodyPublishers.ofString(
                                eventData.toJSONString()
                            )
                        )
                        .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (response.statusCode() >= 200
                && response.statusCode() < 300) {

            System.out.println(
                    "Home Assistant event fired: tuya_camera_human"
            );

        } else {

            System.err.println(
                    "Home Assistant API error: HTTP "
                            + response.statusCode()
                            + " "
                            + response.body()
            );
        }
    }
}