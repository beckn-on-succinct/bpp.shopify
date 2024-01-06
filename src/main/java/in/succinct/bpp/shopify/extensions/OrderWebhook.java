package in.succinct.bpp.shopify.extensions;

import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Registry;
import com.venky.swf.path.Path;
import in.succinct.beckn.Amount;
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
import in.succinct.bpp.shopify.model.ShopifyOrder.ShopifyRefund;
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

        String becknTransactionId = eCommerceAdaptor.getBecknTransactionId(shopifyOrder);

        LocalOrderSynchronizer localOrderSynchronizer = LocalOrderSynchronizerFactory.getInstance().getLocalOrderSynchronizer(eCommerceAdaptor.getSubscriber());
        Order lastKnownOrderState = localOrderSynchronizer.getLastKnownOrder(becknTransactionId,true);
        if (ObjectUtil.equals(path.getHeaders().get("X-Shopify-Topic"),"orders/updated")){
            if (!localOrderSynchronizer.hasOrderReached(becknTransactionId,Status.Accepted) || localOrderSynchronizer.hasOrderReached(becknTransactionId,Status.Completed)){
                return;
            }
        }

        if (lastKnownOrderState.getState() != shopifyOrder.getStatus()){
            if (shopifyOrder.getStatus() == Status.Cancelled){
                //Cancelling. !!
                return;
            }
        }
        if (ObjectUtil.equals(path.getHeaders().get("X-SHOPIFY-TOPIC"),"orders/cancelled")){
            // Let the message be sent via refund hook.!!
            return;
        }
        // All Refunds available!
        boolean allRefundsPresent = true;
        for (ShopifyRefund shopifyRefund : shopifyOrder.getRefunds()){
            allRefundsPresent = allRefundsPresent && lastKnownOrderState.getRefunds().get(shopifyRefund.getId()) != null;
        }
        if (!allRefundsPresent){
            // Let refund hook process this..
            return;
        }
        Order becknOrder = eCommerceAdaptor.getBecknOrder(shopifyOrder); //Fill all attributes here.


        if (becknOrder.getPayment().getStatus() == null){
            return;
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
                becknOrder.setCounterpartyReconStatus(ReconStatus.PAID);
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
                    becknOrder.setCounterpartyReconStatus(ReconStatus.UNDER_PAID);
                    Amount diff = new Amount();
                    diff.setValue(settlementAmountExpected.doubleValue()-shopifyOrder.getSettledAmount());
                    diff.setCurrency("INR");
                    becknOrder.setCounterpartyDiffAmount(diff);
                }else {
                    becknOrder.setCounterpartyReconStatus(ReconStatus.PAID);
                    becknOrder.setCounterpartyDiffAmount(null);
                }
            }
            networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,on_receiver_recon);

        }

        final Request request = new Request();
        request.setMessage(new Message());
        request.setContext(shopifyOrder.getContext());
        request.getMessage().setOrder(becknOrder);
        Context context = request.getContext();
        context.setBppId(eCommerceAdaptor.getSubscriber().getSubscriberId());
        context.setBppUri(eCommerceAdaptor.getSubscriber().getSubscriberUrl());
        context.setAction(event);
        context.setDomain(eCommerceAdaptor.getSubscriber().getDomain());


        //Fill any other attributes needed.
        //Send unsolicited on_status.
        context.setMessageId(UUID.randomUUID().toString());
        context.setTimestamp(becknOrder.getUpdatedAt());
        networkAdaptor.getApiAdaptor().callback(eCommerceAdaptor,request);
    }
}
