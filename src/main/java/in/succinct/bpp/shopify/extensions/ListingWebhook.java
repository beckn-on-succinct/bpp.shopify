package in.succinct.bpp.shopify.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.extension.Registry;
import com.venky.swf.db.model.application.Event;
import com.venky.swf.path.Path;
import in.succinct.beckn.Items;
import in.succinct.beckn.Provider;
import in.succinct.bpp.core.adaptor.ItemFetcher;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.search.extensions.SearchExtensionInstaller;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class ListingWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.listing_hook",new ListingWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path, String payload) {
        String parameter = path.parameter(); //This will be update.

        JSONObject ePayload = (JSONObject) JSONValue.parse(payload);

        String productId = String.valueOf(((JSONObject)ePayload.get("product_listing")).get("product_id"));
        String topic = path.getHeaders().get("X-Shopify-Topic");
        eCommerceAdaptor.clearCache();
        Provider provider = eCommerceAdaptor.getProvider(() -> eCommerceAdaptor.getItems(Collections.singleton(productId)));

        Event event = null;
        if (ObjectUtil.equals(topic,"product_listings/add")) {
            event = Event.find(SearchExtensionInstaller.CATALOG_SYNC_ACTIVATE);
        }else if (ObjectUtil.equals(topic,"product_listings/remove")){
            event = Event.find(SearchExtensionInstaller.CATALOG_SYNC_DEACTIVATE);
        }
        if (event != null ){
            event.raise(provider);
        }


    }
}
