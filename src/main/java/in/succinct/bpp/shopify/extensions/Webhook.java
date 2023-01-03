package in.succinct.bpp.shopify.extensions;

import com.venky.core.security.Crypt;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import com.venky.swf.routing.Config;
import in.succinct.beckn.Context;
import in.succinct.beckn.Order;
import in.succinct.beckn.Message;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;

public class Webhook implements Extension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.hook",new Webhook());
    }
    @Override
    public void invoke(Object... objects) {
        CommerceAdaptor adaptor = (CommerceAdaptor) objects[0];
        Path path = (Path) objects[1];
        if (!(adaptor instanceof ECommerceAdaptor)) {
            return;
        }
        ECommerceAdaptor eCommerceAdaptor = (ECommerceAdaptor) adaptor;
        try {
            hook(eCommerceAdaptor, path);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    public void hook(ECommerceAdaptor eCommerceAdaptor,Path path) throws Exception{
        String payload = StringUtil.read(path.getInputStream());
        //Validate auth headers from path.getHeader
        if (ObjectUtil.equals(path.getHeader("X-WC-Webhook-Topic"),"order.updated")){
            JSONObject eOrder = (JSONObject) JSONValue.parse(payload);
            Order becknOrder = new Order(); //Fill all attributes here. 
           


            final Request request = new Request();
            request.setMessage(new Message());
            request.setContext(new Context());
            request.getMessage().setOrder(becknOrder); 
            Context context = request.getContext();
            context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
            context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
            context.setTimestamp(new Date());
            context.setAction("on_status");
            //Fill any other attributes needed.
            //Send unsolicited on_status.
            context.setMessageId(UUID.randomUUID().toString());
            eCommerceAdaptor.callback(request);

        }
    }
}
