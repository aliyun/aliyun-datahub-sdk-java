package com.aliyun.datahub.model.serialize;

import com.aliyun.datahub.common.transport.Response;
import com.aliyun.datahub.common.util.JacksonParser;
import com.aliyun.datahub.exception.*;
import com.aliyun.datahub.rest.DatahubHttpHeaders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * parse for JSON error responses from Datahub services.
 */
public class JsonErrorParser implements ErrorParser {

    public DatahubServiceException parse(Response response) {

        DatahubServiceException ex = null;
        if (response.getBody() == null) {
            ex = new DatahubServiceException(("Invalid response, empty body."));
        } else {
            try {
                ObjectMapper mapper = JacksonParser.getObjectMapper();

                JsonNode tree = mapper.readTree(response.getBody());
                JsonNode node = tree.get("ErrorCode");
                if (node.isNull()) {
                    ex = new DatahubServiceException(new String(response.getBody()));
                } else {
                    String er = node.asText();
                    if (er.equals("InvalidParameter") || er.equals("InvalidSubscription")) {
                        ex = new InvalidParameterException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("InvalidCursor")) {
                        ex = new InvalidCursorException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("NoSuchTopic") || er.equals("NoSuchProject") || er.equals("NoSuchSubscription")
                            || er.equals("NoSuchShard") || er.equals("NoSuchConnector") || er.equals("NoSuchMeteringInfo")) {
                        ex = new ResourceNotFoundException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("ProjectAlreadyExist")
                            || er.equals("TopicAlreadyExist")
                            || er.equals("ConnectorAlreadyExist")) {
                        ex = new ResourceExistException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("Unauthorized")) {
                        ex = new AuthorizationFailureException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("NoPermission")) {
                        ex = new NoPermissionException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("InvalidShardOperation")) {
                        ex = new InvalidOperationException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("OperationDenied")) {
                        ex = new OperationDeniedException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("LimitExceeded")) {
                        ex = new LimitExceededException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("OdpsServiceError")) {
                        ex = new OdpsException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("MysqlServiceError")) {
                        ex = new MysqlException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("InternalServerError")) {
                        ex = new InternalFailureException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("SubscriptionOffline")) {
                        ex = new SubscriptionOfflineException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("OffsetReseted")) {
                        ex = new OffsetResetedException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("OffsetSessionClosed")) {
                        ex = new OffsetSessionClosedException(tree.get("ErrorMessage").asText());
                    } else if (er.equals("OffsetSessionChanged")) {
                        ex = new OffsetSessionChangedException(tree.get("ErrorMessage").asText());
                    } else {
                        ex = new DatahubServiceException(tree.get("ErrorMessage").asText());
                    }
                    ex.setErrorCode(er);
                }
            } catch (IOException e) {
                ex = new DatahubServiceException(new String(response.getBody()));
            }
        }

        ex.setStatusCode(response.getStatus());
        ex.setRequestId(response.getHeader(DatahubHttpHeaders.HEADER_DATAHUB_REQUEST_ID));
        return ex;
    }

    private static JsonErrorParser instance;

    public static JsonErrorParser getInstance() {
        if (instance == null)
            instance = new JsonErrorParser();
        return instance;
    }
}
