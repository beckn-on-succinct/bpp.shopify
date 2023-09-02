package in.succinct.bpp.shopify.extensions;

import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Registry;
import com.venky.swf.db.annotations.column.COLUMN_DEF;
import com.venky.swf.path.Path;
import in.succinct.beckn.Context;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.OrderReconStatus;
import in.succinct.beckn.Order.Orders;
import in.succinct.beckn.Order.ReconStatus;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizer;
import in.succinct.bpp.core.db.model.LocalOrderSynchronizerFactory;
import in.succinct.bpp.core.db.model.rsp.Settlement;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.model.ShopifyOrder;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Date;
import java.util.List;
import java.util.UUID;

public class OrderWebhook extends ShopifyWebhook{
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.order_hook",new OrderWebhook());
    }

    public void hook(ECommerceAdaptor eCommerceAdaptor, NetworkAdaptor networkAdaptor,Path path, String payload) {
        String event = path.parameter();

        JSONObject eOrder = (JSONObject) JSONValue.parse(payload);
        ShopifyOrder shopifyOrder = new ShopifyOrder(eOrder);

        // Check old order here and if old order is not settled an now it is settled. Also send on_receiver_recon
        String becknTransactionId = eCommerceAdaptor.getBecknTransactionId(shopifyOrder);
        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber());

        Order lastKnownOrderState = localOrderSynchronizer.getLastKnownOrder(becknTransactionId);
        if (ObjectUtil.equals(path.getHeaders().get("X-Shopify-Topic"),"orders/updated")){
            if (!localOrderSynchronizer.hasOrderReached(becknTransactionId,Status.Accepted) || localOrderSynchronizer.hasOrderReached(becknTransactionId,Status.Completed)){
                return;
            }
        }


        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder); //Fill all attributes here.
        if (becknOrder.getState() == Status.Cancelled && !localOrderSynchronizer.hasOrderReached(becknTransactionId,Status.Cancelled) ){
            event = "on_cancel";
        }
        if (lastKnownOrderState.getReconStatus() == ReconStatus.PAID){
            Request on_receiver_recon = new Request();
            Context context = new Context();
            on_receiver_recon.setContext(context);
            context.setNetworkId(networkAdaptor.getId());
            context.setAction("on_receiver_recon");
            context.setBppId(becknOrder.getReceiverSubscriberId());
            context.setBapId(becknOrder.getCollectorSubscriberId());
            context.setDomain("NTS10");
            context.setTimestamp(new Date());
            context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
            on_receiver_recon.setMessage(new Message());
            Orders orders = new Orders();
            on_receiver_recon.getMessage().setOrders(orders);
            orders.add(becknOrder);

            if (shopifyOrder.isSettled()){
                becknOrder.setOrderReconStatus(OrderReconStatus.FINALE);
                becknOrder.setCounterPartyReconStatus(ReconStatus.PAID);
            }else if (shopifyOrder.getSettledAmount() > 0){
                List<Settlement> settlementList = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber()).getSettlements(becknTransactionId);
                Bucket settlementAmountExpected = new Bucket();
                Settlement current = null;
                for (Settlement settlement : settlementList) {
                    settlementAmountExpected.increment(settlement.getExpectedCreditInBank());
                    if (settlement.getSettlementReference().equals(becknOrder.getSettlementReference())){
                        current = settlement;
                    }
                }


                if (current != null) {
                    becknOrder.setCollectionTransactionId(current.getCollectionTxnId());
                }
                becknOrder.setOrderReconStatus(OrderReconStatus.FINALE);
                if (settlementAmountExpected.doubleValue() > shopifyOrder.getSettledAmount()){
                    becknOrder.setCounterPartyReconStatus(ReconStatus.UNDER_PAID);
                    becknOrder.setCounterPartyDiffAmount(settlementAmountExpected.doubleValue()-shopifyOrder.getSettledAmount());
                }else {
                    becknOrder.setCounterPartyReconStatus(ReconStatus.PAID);
                    becknOrder.setCounterPartyDiffAmount(null);
                }
            }
            networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,on_receiver_recon);

        }

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
