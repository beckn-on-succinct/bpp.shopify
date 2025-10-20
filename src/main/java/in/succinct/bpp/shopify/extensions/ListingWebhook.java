package in.succinct.bpp.shopify.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.extension.Registry;
import com.venky.swf.db.model.application.Event;
import com.venky.swf.path.Path;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Context;
import in.succinct.beckn.Message;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.catalog.indexer.ingest.CatalogDigester.Operation;
import in.succinct.onet.core.adaptor.NetworkAdaptor;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Collections;
import java.util.Date;
import java.util.UUID;

public class ListingWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.listing_hook",new ListingWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor, Path path, String payload) {
        String parameter = path.parameter(); //This will be update.

        JSONObject ePayload = (JSONObject) JSONValue.parse(payload);

        String productId = String.valueOf(((JSONObject)ePayload.get("product_listing")).get("product_id"));
        String topic = path.getHeaders().get("X-Shopify-Topic");
        eCommerceAdaptor.clearCache();
        Provider provider = eCommerceAdaptor.getProvider(() -> eCommerceAdaptor.getItems(Collections.singleton(productId)));

        Operation operation = null;
        if (ObjectUtil.equals(topic,"product_listings/add")) {
            operation = Operation.activate;
        }else if (ObjectUtil.equals(topic,"product_listings/remove")){
            operation = Operation.deactivate;
        }
        if (operation != null ){
            Providers providers = new Providers();
            providers.add(provider);
            raiseEvent(operation,providers,eCommerceAdaptor,networkAdaptor);
        }


    }
    public void raiseEvent(Operation operation, Providers providers, CommerceAdaptor adaptor,NetworkAdaptor networkAdaptor){
        Event event = Event.find("catalog_" + operation.name());
        Request request = new Request();
        Context context = new Context();
        request.setContext(context);
        request.setMessage(new Message());
        request.getMessage().setCatalog(new Catalog());
        request.getMessage().getCatalog().setProviders(providers);
        context.setBppId(adaptor.getSubscriber().getSubscriberId());
        context.setBppUri(adaptor.getSubscriber().getSubscriberUrl());
        context.setTransactionId(UUID.randomUUID().toString());
        context.setMessageId(UUID.randomUUID().toString());
        context.setDomain(adaptor.getSubscriber().getDomain());
        context.setCountry(adaptor.getSubscriber().getCountry());
        context.setCoreVersion(networkAdaptor.getCoreVersion());
        context.setTimestamp(new Date());
        context.setNetworkId(networkAdaptor.getId());
        context.setCity(adaptor.getSubscriber().getCity());
        context.setAction("on_search");
        context.setTtl(60L);
        for (in.succinct.beckn.Provider provider : providers){
            provider.setTag("general_attributes","catalog.indexer.reset","N");
            provider.setTag("general_attributes","catalog.indexer.operation",operation.name());
        }

        event.raise(request);

    }
}
