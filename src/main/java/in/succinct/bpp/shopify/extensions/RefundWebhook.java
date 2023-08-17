package in.succinct.bpp.shopify.extensions;

import com.venky.core.security.Crypt;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper;
import com.venky.swf.path.Path;
import in.succinct.beckn.Amount;
import in.succinct.beckn.Context;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Return.ReturnStatus;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;

public class RefundWebhook extends ShopifyWebhook {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.refund_hook",new RefundWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path, String payload) {
        String event = path.parameter();

        JSONObject ePayload = (JSONObject) JSONValue.parse(payload);
        JSONObject eReturn = (JSONObject) ePayload.get("return");
        JSONObject eOrder = (JSONObject) ePayload.get("order");

        String refundId = StringUtil.valueOf(ePayload.get("id"));
        String orderId = StringUtil.valueOf(eOrder.get("id"));
        String returnId = StringUtil.valueOf(eReturn.get("Id"));
        JSONObject amount = (JSONObject) (((JSONObject)ePayload.get("totalRefundSet")).get("presentmentMoney"));


        ShopifyOrder shopifyOrder = eCommerceAdaptor.getShopifyOrder(orderId);
        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder);

        Return returnReference = becknOrder.getReturns().get(returnId);
        returnReference.setReturnStatus(ReturnStatus.REFUNDED);
        returnReference.setRefund(new Amount());
        returnReference.getRefund().setCurrency((String)amount.get("currencyCode"));
        returnReference.getRefund().setValue(Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter().valueOf(amount.get("amount")));



        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(new Context());
        request.getMessage().setOrder(becknOrder);
        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setTimestamp(new Date());
        context.setAction(event);
        context.setDomain(eCommerceAdaptor.getSubscriber().getDomain());
        shopifyOrder.getNoteAttributes().forEach(na->{
            if (na.getName().startsWith("context.")){
                String key = na.getName().substring("context.".length());
                context.set(key,na.getValue());
            }
        });


        //Fill any other attributes needed.
        //Send unsolicited on_status.
        context.setMessageId(UUID.randomUUID().toString());
        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
