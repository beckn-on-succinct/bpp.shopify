package in.succinct.bpp.shopify.model;

import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.Order.Status;
import in.succinct.bpp.shopify.model.Products.Metafields;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Order extends DraftOrder{
    public Order(){
        super();
    }
    public Order(JSONObject eCommerceOrder) {
        super(eCommerceOrder);
    }




}
