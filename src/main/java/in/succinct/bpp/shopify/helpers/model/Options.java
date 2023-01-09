package in.succinct.bpp.shopify.helpers.model;

import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Option;
import org.json.simple.JSONArray;

public class Options extends BecknObjectsWithId<Option> {


    public Options() {
    }

    public Options(JSONArray array) {
        super(array);
    }

    public static class Option extends ShopifyObjectWithId{
        public String getName(){
            return get("name");
        }

        public int getPosition(){
            return getInteger("position");
        }

        public BecknStrings getValues(){
            return get(BecknStrings.class, "values");
        }


    }


}
