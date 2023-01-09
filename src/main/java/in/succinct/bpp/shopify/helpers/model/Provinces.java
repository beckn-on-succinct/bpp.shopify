package in.succinct.bpp.shopify.helpers.model;

import com.venky.core.string.StringUtil;
import in.succinct.beckn.BecknObjectWithId;
import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.bpp.shopify.helpers.model.Provinces.Province;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Provinces extends BecknObjectsWithId<Province> {


    public Provinces(JSONArray array) {
        super(array);
    }

    public static class Province extends ShopifyObjectWithId{

        public Province() {
        }

        public Province(JSONObject object) {
            super(object);
        }

        public String getCode(){
            return get("code");
        }

        public String getCountryId(){
            return StringUtil.valueOf(get("country_id"));
        }

        public String getName(){
            return get("name");
        }


    }
}
