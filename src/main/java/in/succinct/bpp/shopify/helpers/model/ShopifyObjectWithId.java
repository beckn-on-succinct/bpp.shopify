package in.succinct.bpp.shopify.helpers.model;

import com.venky.core.string.StringUtil;
import in.succinct.beckn.BecknObjectWithId;
import org.json.simple.JSONObject;

public class ShopifyObjectWithId extends BecknObjectWithId {

    public ShopifyObjectWithId() {
    }

    public ShopifyObjectWithId(String payload) {
        super(payload);
    }

    public ShopifyObjectWithId(JSONObject object) {
        super(object);
    }

    @Override
    public String getId() {
        return StringUtil.valueOf(get("id"));
    }
}
