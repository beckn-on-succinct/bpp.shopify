package in.succinct.bpp.shopify.adaptor;

import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentType;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Intent;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.registry.BecknRegistry;
import in.succinct.bpp.search.adaptor.SearchAdaptor;
import in.succinct.bpp.shopify.db.model.BecknOrderMeta;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.Map;

public class ECommerceAdaptor extends CommerceAdaptor {
    final SearchAdaptor searchAdaptor;
    final Map<String,String> configuration ;
    public ECommerceAdaptor(Map<String,String> configuration, Subscriber subscriber, BecknRegistry registry){
        super(subscriber,registry);
        this.searchAdaptor = new SearchAdaptor(this);
        this.configuration = configuration;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public void search(Request request,Request reply){
        this.searchAdaptor.search(request,reply);
    }

    /* Don't remove Used from AppInstaller */
    /* Search is fulfilled from the plugin */
    public void _search(Request request, Request reply) {
        /* Code here logic to seach your ecommerce application like woocommerce,shopify, ...*/
        /* Return reply compatible with on_search json */
    }

    @Override
    public void select(Request request, Request response) {
        /* Take select json and fill response with on_select message */

    }

    @Override
    public void init(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take init message and fill response with on_init message */

    }

    @Override
    public void confirm(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take confirm message and fill response with on_confirm message */
    }

    @Override
    public void track(Request request, Request reply) {
        /* Take track message and fill response with on_track message */

    }

    @Override
    public void cancel(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take cancel message and fill response with on_cancel message */
    }

    @Override
    public void update(Request request, Request reply) {
        throw new RuntimeException("Orders cannot be updated. Please cancel and rebook your orders!");
    }

    @Override
    public void status(Request request, Request reply) {
        Order order = request.getMessage().getOrder();
        if (order == null){
            order = new Order();
            order.setId(request.getMessage().get("order_id"));
            request.getMessage().setOrder(order);
        }
        if (order == null){
            throw new RuntimeException("No Order passed");
        }
        /* Take status message and fill response with on_status message */

    }

    @Override
    public void rating(Request request, Request reply) {

    }


    @Override
    public void support(Request request, Request reply) {
        
    }

    @Override
    public void get_cancellation_reasons(Request request, Request reply) {

    }

    @Override
    public void get_return_reasons(Request request, Request reply) {

    }

    @Override
    public void get_rating_categories(Request request, Request reply) {

    }

    @Override
    public void get_feedback_categories(Request request, Request reply) {

    }

    @Override
    public void get_feedback_form(Request request, Request reply) {

    }


}
