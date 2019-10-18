package com.hurence.logisland.processor.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.hurence.logisland.controller.ControllerService;

/**
 * This interface defines how clients interact with an S3 encryption service.
 */
public interface AmazonS3EncryptionService extends ControllerService {

    String STRATEGY_NAME_NONE = "NONE";
    String STRATEGY_NAME_SSE_S3 = "SSE_S3";
    String STRATEGY_NAME_SSE_KMS = "SSE_KMS";
    String STRATEGY_NAME_SSE_C = "SSE_C";
    String STRATEGY_NAME_CSE_KMS = "CSE_KMS";
    String STRATEGY_NAME_CSE_C = "CSE_C";

    /**
     * Configure a {@link PutObjectRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configurePutObjectRequest(PutObjectRequest request, ObjectMetadata objectMetadata);

    /**
     * Configure an {@link InitiateMultipartUploadRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configureInitiateMultipartUploadRequest(InitiateMultipartUploadRequest request, ObjectMetadata objectMetadata);

    /**
     * Configure a {@link GetObjectRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configureGetObjectRequest(GetObjectRequest request, ObjectMetadata objectMetadata);

    /**
     * Configure an {@link UploadPartRequest} for encryption.
     * @param request the request to configure.
     * @param objectMetadata the request metadata to configure.
     */
    void configureUploadPartRequest(UploadPartRequest request, ObjectMetadata objectMetadata);

    /**
     * Create an S3 encryption client.
     *
     * @param credentialsProvider AWS credentials provider.
     * @param clientConfiguration Client configuration.
     * @return {@link AmazonS3Client}, perhaps an {@link com.amazonaws.services.s3.AmazonS3EncryptionClient}
     */
    AmazonS3Client createEncryptionClient(AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration);

    /**
     * @return The KMS region associated with the service, as a String.
     */
    String getKmsRegion();

    /**
     * @return The name of the encryption strategy associated with the service.
     */
    String getStrategyName();

    /**
     * @return The display name of the encryption strategy associated with the service.
     */
    String getStrategyDisplayName();
}
