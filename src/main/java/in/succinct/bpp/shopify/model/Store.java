package in.succinct.bpp.shopify.model;

import com.venky.geo.GeoCoordinate;
import org.json.simple.JSONObject;

import java.util.Date;

public class Store extends ShopifyObjectWithId {
    public Store(){

    }

    public Store(JSONObject object) {
        super(object);
    }

    public String getAddress1(){
        return get("address1");
    }

    public String getAddress2(){
        return get("address2");
    }

    public String getCity(){
        return get("city");
    }

    public String getCountryCode(){
        return get("country_code");
    }

    public Date getCreatedAt(){
        return getTimestamp("created_at");
    }

    public String getSupportEmail(){
        return get("customer_email");
    }

    public String getCurrency(){
        return get("currency");
    }

    public String getDomain(){
        return get("domain");
    }

    public String getAdminEmail(){
        return get("email");
    }

    public double getLatitude(){
        return getDouble("latitude");
    }
    public double getLongitude(){
        return getDouble("longitude");
    }

    public GeoCoordinate getGeoCoordinate(){
        return new GeoCoordinate(getLatitude(),getLongitude());
    }

    public String getName(){
        return get("name");
    }

    public String getPhone(){
        return get("phone");
    }

    public String getProvince(){
        return get("province");
    }

    public String getProvinceCode(){
        return get("province_code");
    }

    public String getShopOwner(){
        return get("shop_owner");
    }

    public boolean isTaxesIncluded(){
        return getBoolean("taxes_included");
    }

    public boolean isTaxShipping(){
        return getBoolean("tax_shipping");
    }

    public String getZip(){
        return get("zip");
    }

    public String getWeightUnit(){
        return get("weight_unit");
    }

    public Date getUpdatedAt(){
        return getTimestamp("updated_at");
    }

    public long getPrimaryLocationId(){
        return getLong("primary_location_id");
    }


}
