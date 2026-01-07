package com.mercari.solution.util.domain.text.template;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.mercari.solution.util.cloud.google.IAMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class GcpFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(GcpFunctions.class);

    public String project() {
        try {
            return Optional
                    .ofNullable(IAMUtil.getProject())
                    .orElse("");
        } catch (final Throwable e) {
            return "";
        }
    }

    public String serviceAccount() {
        try {
            return Optional
                    .ofNullable(IAMUtil.getServiceAccount())
                    .orElse("");
        } catch (final Throwable e) {
            return "";
        }
    }

    public String accessToken() {
        try {
            final GoogleCredentials credential = GoogleCredentials.getApplicationDefault();
            if(credential == null) {
                return "";
            }
            AccessToken accessToken = credential.getAccessToken();
            if(accessToken == null) {
                accessToken = credential.refreshAccessToken();
            }
            if(accessToken == null) {
                return "";
            }
            return Optional
                    .ofNullable(accessToken.getTokenValue())
                    .orElse("");
        } catch (final Throwable e) {
            return "";
        }
    }

    public String signJwt(String serviceAccount, String... params) {
        try {
            final JsonObject payload = new JsonObject();
            if(!IAMUtil.isServiceAccountResource(serviceAccount)) {
                if(IAMUtil.isMail(serviceAccount)) {
                    serviceAccount = "projects/-/serviceAccounts/" + serviceAccount;
                } else {
                    LOG.error("Illegal serviceAccount(not mail format): {}", serviceAccount);
                    return "";
                }
            }

            if(params.length % 2 == 1) {
                LOG.error("params length must be even. but odd length: {}", params.length);
                return "";
            }

            for(int i=0; i<params.length; i+=2) {
                if("exp".equals(params[i])) {
                    payload.addProperty(params[i], Integer.parseInt(params[i+1]));
                } else {
                    payload.addProperty(params[i], params[i+1]);
                }
            }
            return IAMUtil.signJwt(serviceAccount, payload);
        } catch (final Throwable e) {
            LOG.error("failed to signJwt serviceAccount: {}, cause: {}", serviceAccount, e.getMessage());
            return "";
        }
    }

}
