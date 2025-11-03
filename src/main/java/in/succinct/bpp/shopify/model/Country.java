package in.succinct.bpp.shopify.model;

import org.json.simple.JSONObject;

public class Country extends ShopifyObjectWithId {
    public Country(){

    }
    public Country(JSONObject object) {
        super(object);
    }

    public String getCode(){
        return get("code");
    }
    public String getName(){
        return get("name");
    }

    public Provinces getProvinces(){
        return get(Provinces.class, "provinces");
    }



}
