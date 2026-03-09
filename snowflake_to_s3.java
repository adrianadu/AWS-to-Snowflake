package com.AWS_TO_Snowflake;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

public class DynamicS3Migration {

    private static final Region REGION = Region.AP_SOUTH_1;
    private static final String BUCKET_NAME = "";
    private static final String FOLDER_PREFIX = "";

    public static void main(String[] args) throws Exception {

        String url = "";
        String user = "";
        String password = "";
        String database = "";

        String storageIntegration = "";
        String fileFormat = "";

        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement();

        stmt.execute("USE DATABASE " + database);

        List<String> tables = new ArrayList<>();

        ResultSet schemas = stmt.executeQuery(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");

        while (schemas.next()) {

            String schema = schemas.getString("SCHEMA_NAME");

            if (schema.equals("PUBLIC") || schema.equals("INFORMATION_SCHEMA"))
                continue;

            ResultSet tableResult = stmt.executeQuery(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "'");

            while (tableResult.next()) {

                String table = schema + "." + tableResult.getString("TABLE_NAME");
                tables.add(table);

                System.out.println("Found table: " + table);
            }
        }

        int count = tables.size();
        System.out.println("Total tables: " + count);

        AwsCredentialsProvider credentialsProvider =
                StaticCredentialsProvider.create(AwsBasicCredentials.create("", ""));

        S3Client s3 = S3Client.builder()
                .region(REGION)
                .credentialsProvider(credentialsProvider)
                .build();

        s3.createBucket(CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build());

        System.out.println("Bucket created");

        List<String> bucketLocations = new ArrayList<>();

        for (int i = 0; i < count; i++) {

            String folder = FOLDER_PREFIX + i;

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET_NAME)
                            .key(folder)
                            .build(),
                    RequestBody.fromByteBuffer(ByteBuffer.wrap(new byte[0]))
            );

            String location = "s3://" + BUCKET_NAME + "/" + folder;

            bucketLocations.add(location);

            System.out.println("Created S3 location: " + location);
        }

        for (int i = 0; i < tables.size(); i++) {

            String table = tables.get(i);
            String location = bucketLocations.get(i);
            String stage = "stage_" + i;

            String createStage =
                    "CREATE OR REPLACE STAGE " + stage +
                            " STORAGE_INTEGRATION = " + storageIntegration +
                            " FILE_FORMAT = " + fileFormat +
                            " URL = '" + location + "'";

            stmt.execute(createStage);

            System.out.println("Stage created: " + stage);

            String copy =
                    "COPY INTO @" + stage +
                            " FROM " + table +
                            " OVERWRITE = TRUE";

            stmt.execute(copy);

            System.out.println("Copied table: " + table + " to " + location);
        }

        conn.close();
        s3.close();

        System.out.println("Migration Completed");
    }
}
