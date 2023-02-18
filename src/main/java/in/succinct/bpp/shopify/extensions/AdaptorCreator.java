package in.succinct.bpp.shopify.extensions;

import com.venky.core.util.ObjectHolder;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;

import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;

import java.util.Map;

public class AdaptorCreator implements Extension {
    static {
        Registry.instance().registerExtension(CommerceAdaptor.class.getName(),new AdaptorCreator());
    }
    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Object... context) {
        Map<String,String> properties = (Map<String,String>) context[0];
        Subscriber subscriber = (Subscriber) context[1];
        ObjectHolder<CommerceAdaptor> commerceAdaptorHolder = (ObjectHolder<CommerceAdaptor>) context[2];
        if (properties.containsKey("in.succinct.bpp.shopify.storeUrl")){
            commerceAdaptorHolder.set(new ECommerceAdaptor(properties,subscriber));
        }
    }

}
