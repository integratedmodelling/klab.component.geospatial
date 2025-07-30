package org.integratedmodelling.geospatial.adapters.stac;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONObject;
import org.integratedmodelling.klab.api.exceptions.KlabResourceAccessException;

public class STACManager {
    public static JSONObject requestMetadata(String collectionUrl, String type) {
        HttpResponse<JsonNode> response = Unirest.get(collectionUrl).asJson();
        if (!response.isSuccess() || response.getBody() == null) {
            throw new KlabResourceAccessException("Cannot access the " + type + " at " + collectionUrl);
        }
        JSONObject data = response.getBody().getObject();
        if (!data.getString("type").equalsIgnoreCase(type)) {
            throw new KlabResourceAccessException("Data at " + collectionUrl + " is not a valid STAC " + type);
        }
        return response.getBody().getObject();
    }

}
