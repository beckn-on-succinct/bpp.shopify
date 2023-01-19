package in.succinct.bpp.shopify.helpers;

import com.venky.cache.Cache;
import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.math.DoubleUtils;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.db.model.application.ApplicationUtil;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.collab.db.model.config.Country;
import com.venky.swf.plugins.collab.db.model.config.State;
import com.venky.swf.plugins.gst.db.model.assets.AssetCode;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Address;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Billing;
import in.succinct.beckn.BreakUp;
import in.succinct.beckn.BreakUp.BreakUpElement;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Contact;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Images;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payment.Params;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Person;
import in.succinct.beckn.Price;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.QuantitySummary;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.beckn.Tag;
import in.succinct.beckn.Tags;
import in.succinct.beckn.User;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.db.model.BecknOrderMeta;
import in.succinct.bpp.shopify.helpers.BecknIdHelper.Entity;
import in.succinct.bpp.shopify.helpers.model.DraftOrder;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.LineItem;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.LineItems;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.NoteAttributes;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.PaymentSchedule;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.PaymentSchedules;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.PaymentTerms;
import in.succinct.bpp.shopify.helpers.model.DraftOrder.TaxLines;
import in.succinct.bpp.shopify.helpers.model.ProductImages;
import in.succinct.bpp.shopify.helpers.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.helpers.model.Products;
import in.succinct.bpp.shopify.helpers.model.Products.InventoryItem;
import in.succinct.bpp.shopify.helpers.model.Products.InventoryItems;
import in.succinct.bpp.shopify.helpers.model.Products.InventoryLevel;
import in.succinct.bpp.shopify.helpers.model.Products.InventoryLevels;
import in.succinct.bpp.shopify.helpers.model.Products.Product;
import in.succinct.bpp.shopify.helpers.model.Products.ProductVariant;
import in.succinct.bpp.shopify.helpers.model.Store;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ECommerceHelper {
    final ECommerceAdaptor adaptor;
    final Application application ;
    public ECommerceHelper(ECommerceAdaptor adaptor){
        this.adaptor = adaptor;
        this.application = adaptor == null ? null : ApplicationUtil.find(adaptor.getSubscriber().getAppId());
    }
    public ECommerceHelper(){
        this(null);
    }
    public String getConfigPrefix(){
        return "in.succinct.bpp.shopify";
    }


    public String getStoreUrl(){
        return adaptor.getConfiguration().get(String.format("%s.storeUrl",getConfigPrefix()));
    }
    public String getAdminApiUrl(){
        return String.format("/admin/api/%s", new SimpleDateFormat("YYYY-MM").format(new Date()));
    }
    public String getAccessToken(){
        return adaptor.getConfiguration().get(String.format("%s.accessToken",getConfigPrefix()));
    }

    public Message createMessage(Request request){
        Message message = new Message();
        request.setMessage(message);
        return message;
    }

    public Catalog createCatalog(Message message) {
        Catalog catalog = new Catalog();
        message.setCatalog(catalog);
        createProviders(catalog);
        return catalog;
    }


    private Providers createProviders(Catalog catalog) {
        Providers providers = new Providers();
        providers.add(getProvider());
        catalog.setProviders(providers);
        return providers;
    }

    private  Provider getProvider(){
        Provider provider = new Provider();
        provider.setDescriptor(new Descriptor());
        provider.getDescriptor().setName(adaptor.getSubscriber().getSubscriberId());
        provider.getDescriptor().setShortDesc(provider.getDescriptor().getName());
        provider.getDescriptor().setLongDesc(adaptor.getConfiguration().get(String.format("%s.provider.name",provider.getDescriptor().getShortDesc())));
        provider.setId(BecknIdHelper.getBecknId(adaptor.getSubscriber().getSubscriberId(), adaptor.getSubscriber().getSubscriberId(), Entity.provider));
        provider.setTtl(120);
        provider.setLocations(getProviderLocations());
        provider.setPayments(getPayments());

        createItems(provider);
        return provider;
    }
    private Payments getPayments(){
        Payments payments = new Payments();
        Payment payment = new Payment();
        payment.setId("1");
        payment.setType("ON-ORDER");
        payment.set("collected_by","BAP");
        payments.add(payment);
        return payments;
    }
    private InventoryLevels getInventoryLevels(Provider provider){
        StringBuilder locationIds = new StringBuilder();
        for (Iterator <Location> i = provider.getLocations().iterator() ; i .hasNext() ; ) {
            Location location = i.next();
            locationIds.append(location.getId());
            if (i.hasNext()){
                locationIds.append(",");
            }
        }
        JSONObject input = new JSONObject();
        input.put("location_ids",locationIds.toString());
        input.put("limit" , 250);

        InventoryLevels levels = new InventoryLevels();
        Page<JSONObject> pageInventoryLevels= new Page<>();
        pageInventoryLevels.next= "/inventory_levels" ;
        while (pageInventoryLevels.next != null){
            pageInventoryLevels = getPage(pageInventoryLevels.next,input);
            JSONArray inventoryLevels = (JSONArray) pageInventoryLevels.data.get("inventory_levels");
            inventoryLevels.forEach(o->{
                levels.add(new InventoryLevel((JSONObject)o));
            });
            input.remove("location_ids");
        }
        return levels;
    }

    private InventoryItems getInventoryItems(Set<Long> ids){
        StringBuilder inventoryItemIds = new StringBuilder();
        for (Iterator <Long> i = ids.iterator() ; i .hasNext() ; ) {
            inventoryItemIds.append(i.next());
            if (i.hasNext()){
                inventoryItemIds.append(",");
            }
        }
        JSONObject input = new JSONObject();
        input.put("ids",inventoryItemIds.toString());
        input.put("limit" , 250);

        InventoryItems inventoryItems = new InventoryItems();
        Page<JSONObject> pageInventoryItems= new Page<>();
        pageInventoryItems.next= "/inventory_items" ;
        while (pageInventoryItems.next != null){
            pageInventoryItems = getPage(pageInventoryItems.next,input);
            JSONArray inventoryItemsArray = (JSONArray) pageInventoryItems.data.get("inventory_items");
            inventoryItemsArray.forEach(o->{
                inventoryItems.add(new InventoryItem((JSONObject)o));
            });
            input.remove("ids");
        }
        return inventoryItems;
    }

    private Items createItems(Provider provider) {
        Items items = new Items();
        provider.setItems(items);

        InventoryLevels levels = getInventoryLevels(provider);
        Cache<Long,List<Long>> inventoryLocations = new Cache<>(0,0) {
            @Override
            protected List<Long> getValue(Long aLong) {
                return new ArrayList<>();
            }
        };
        levels.forEach(level-> inventoryLocations.get(level.getInventoryItemId()).add(level.getLocationId()));

        InventoryItems inventoryItems = getInventoryItems(inventoryLocations.keySet());

        JSONObject input = new JSONObject();
        input.put("limit",250);
        Page<JSONObject> productsPage = new Page<>();
        productsPage.next="/products.json";

        Map<String,Double> taxRateMap = getTaxRateMap();

        while (productsPage.next != null) {
            productsPage = getPage(productsPage.next, input);
            Products products = new Products((JSONArray) productsPage.data.get("products"));
            for (Product product : products) {
                if (!product.isActive()){
                    continue;
                }

                for (ProductVariant variant : product.getProductVariants()){
                    inventoryLocations.get(variant.getInventoryItemId()).forEach(location_id ->{

                        Item item  = new Item();
                        item.setId(BecknIdHelper.getBecknId(variant.getId(),adaptor.getSubscriber().getSubscriberId(),Entity.item));

                        Descriptor descriptor = new Descriptor();
                        item.setDescriptor(descriptor);
                        descriptor.setCode(variant.getBarCode());
                        descriptor.setShortDesc(variant.getTitle());
                        descriptor.setLongDesc(variant.getTitle());
                        descriptor.setImages(new Images());
                        ProductImages productImages = product.getImages();

                        if (variant.getImageId() > 0){
                            ProductImage image = productImages.get(StringUtil.valueOf(variant.getImageId()));
                            if (image != null) {
                                descriptor.getImages().add(image.getSrc());
                            }
                        }else if (productImages.size() > 0){
                            for (ProductImage i : productImages){
                                descriptor.getImages().add(i.getSrc());
                            }
                        }
                        item.setCategoryId(BecknIdHelper.getBecknId(product.getProductType(),adaptor.getSubscriber().getSubscriberId(),Entity.category));
                        item.setCategoryIds(new BecknStrings());
                        item.getCategoryIds().add(item.getCategoryId());

                        item.setTags(new Tags());

                        StringTokenizer tokenizer = new StringTokenizer(product.getTags(),",");
                        while (tokenizer.hasMoreTokens()) {
                            item.getTags().set(tokenizer.nextToken(),"true");
                        }
                        InventoryItem inventoryItem = inventoryItems.get(StringUtil.valueOf(variant.getInventoryItemId()));
                        item.getTags().set("hsn_code",inventoryItem.getHarmonizedSystemCode());
                        item.getTags().set("country_of_origin", inventoryItem.getCountryCodeOfOrigin());
                        item.getTags().set("tax_rate", taxRateMap.get(inventoryItem.getHarmonizedSystemCode()));
                        item.getTags().set("product_id",variant.getProductId());

                        item.setLocationId(BecknIdHelper.getBecknId(StringUtil.valueOf(location_id),adaptor.getSubscriber().getSubscriberId(),Entity.provider_location));
                        item.setLocationIds(new BecknStrings());
                        item.getLocationIds().add(item.getLocationId());

                        Price price = new Price();
                        item.setPrice(price);
                        price.setMaximumValue(variant.getMrp());
                        price.setListedValue(variant.getPrice());
                        price.setCurrency(store.getCurrency());
                        price.setValue(variant.getPrice());
                        price.setOfferedValue(variant.getPrice());


                        item.setPaymentIds(new BecknStrings());
                        item.getPaymentIds().add(BecknIdHelper.getBecknId("1",adaptor.getSubscriber().getSubscriberId(),Entity.payment)); //Only allow By BAP , ON_ORDER

                        items.add(item);

                    });


                }
            }
        }
        return items;
    }

    private Locations getProviderLocations() {
        Locations locations = new Locations();

        JSONObject response = get("/locations.json",new JSONObject());
        JSONArray stores = (JSONArray) response.get("locations");
        stores.forEach(o->{
            JSONObject store = (JSONObject)o;
            if (!Database.getJdbcTypeHelper("").getTypeRef(Boolean.class).getTypeConverter().valueOf(store.get("active"))){
                return;
            }
            if (!Database.getJdbcTypeHelper("").getTypeRef(Boolean.class).getTypeConverter().valueOf(store.get("legacy"))){
                return;
            }
            Location location = new Location();
            location.setId(BecknIdHelper.getBecknId((String)store.get("id"),adaptor.getSubscriber().getSubscriberId(),Entity.provider_location));
            location.setAddress(new Address());
            location.getAddress().setName((String)store.get("name"));
            location.getAddress().setCity((String)store.get("city"));
            location.getAddress().setPinCode((String)store.get("zip"));
            location.getAddress().setCountry((String)store.get("country"));
            location.getAddress().setState((String)store.get("province"));
            locations.add(location);
        });
        return locations;
    }

    Store store = null;
    public Store getStore(){
        if (store == null) {
            JSONObject storeJson = get("/shop.json", new JSONObject());
            store = new Store((JSONObject) storeJson.get("store"));
        }
        return store;
    }


    public Order createOrder(Message message){
        Order order = new Order();
        message.setOrder(order);
        return order;
    }

    public Quote createQuote(Order outOrder, Order inOrder){
        Quote quote = new Quote();
        outOrder.setQuote(quote);
        addItemsToQuote(outOrder,inOrder.getItems());
        return quote;
    }

    public boolean isTaxIncludedInPrice(){
        return getStore().getTaxesIncluded();
    }


    private TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private TypeConverter<Boolean> booleanTypeConverter  = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();

    public Map<String,Double> getTaxRateMap(){
        Map<String,Double> taxRateMap = new Cache<>(0,0) {
            @Override
            protected Double getValue(String taxClass) {
                if (!ObjectUtil.isVoid(taxClass)){
                    AssetCode assetCode = AssetCode.find(taxClass);
                    return assetCode.getGstPct();
                }
                return 0.0D;
            }
        };
        return taxRateMap;
    }

    private in.succinct.bpp.search.db.model.Item getItem(String objectId){
        Select select = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        List<in.succinct.bpp.search.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(),"APPLICATION_ID", Operator.EQ,application.getId())).
                add(new Expression(select.getPool(),"OBJECT_ID",Operator.EQ,objectId))).execute(1);

        in.succinct.bpp.search.db.model.Item dbItem = dbItems.isEmpty()? null : dbItems.get(0);
        return dbItem;
    }

    public Items addItemsToQuote(Order order, Items inItems) {
        Items items = new Items();
        order.setItems(items);

        Quote quote = order.getQuote();
        Price orderPrice = new Price();
        BreakUp breakUp = new BreakUp();
        quote.setPrice(orderPrice);
        quote.setBreakUp(breakUp);

        BreakUpElement product_total = breakUp.createElement("item","Product Total", new Price());
        BreakUpElement product_tax_total = breakUp.createElement("item","Product Tax", new Price());
        BreakUpElement shipping_total = breakUp.createElement("fulfillment","Shipping Total", new Price());
        BreakUpElement shipping_tax_total = breakUp.createElement("fulfillment","Shipping Tax", new Price());
        breakUp.add(product_total);
        breakUp.add(product_tax_total);
        breakUp.add(shipping_total);
        breakUp.add(shipping_tax_total);


        for (int i = 0 ; i < inItems.size() ; i ++ ){
            Item inItem = inItems.get(i);

            String localId = BecknIdHelper.getLocalUniqueId(inItem.getId(), Entity.item);


            Quantity quantity = inItem.get(Quantity.class,"quantity");


            in.succinct.bpp.search.db.model.Item dbItem = getItem(inItem.getId());
            if (dbItem == null ){
                throw new RuntimeException("No inventory with provider.");
            }
            Item outItem = new Item(dbItem.getObjectJson());


            double taxRate = doubleTypeConverter.valueOf(outItem.getTags().get("tax_rate"));

            double configured_price  = outItem.getPrice().getValue(); //may be discounted price

            // price *(1+r/100) = configured_price
            // tax  = price * (r/100) = configured_price * (r/100)/(1+r/100)
            double current_price = isTaxIncludedInPrice() ? configured_price / (1 + taxRate/100.0): configured_price;
            double regular_price = isTaxIncludedInPrice() ? outItem.getPrice().getMaximumValue() / (1 + taxRate/100.0) : outItem.getPrice().getMaximumValue();

            QuantitySummary outQuantity = new QuantitySummary();
            outItem.set("quantity",outQuantity);
            outQuantity.setSelected(quantity);

            Price price = new Price();
            outItem.setPrice(price);
            price.setCurrency("INR");
            price.setListedValue(regular_price * quantity.getCount());
            price.setOfferedValue(current_price * quantity.getCount());
            price.setValue(current_price * quantity.getCount());

            Price p = product_total.get(Price.class,"price");
            p.setCurrency("INR");
            p.setListedValue(p.getListedValue() + price.getListedValue());
            p.setOfferedValue(p.getOfferedValue() + price.getOfferedValue());
            p.setValue(p.getValue() + price.getValue());

            Price t = product_tax_total.get(Price.class,"price");
            t.setCurrency("INR");

            t.setValue(t.getValue() + (taxRate/100.0 * price.getValue()));
            t.setListedValue(t.getListedValue() + (taxRate/100.0) * price.getListedValue());
            t.setOfferedValue(t.getOfferedValue() + (taxRate/100.0) * price.getOfferedValue());

            items.add(outItem);
        }



        orderPrice.setListedValue(product_total.get(Price.class,"price").getListedValue() + product_tax_total.get(Price.class,"price").getListedValue());
        orderPrice.setOfferedValue(product_total.get(Price.class,"price").getOfferedValue() + product_tax_total.get(Price.class,"price").getOfferedValue());
        orderPrice.setValue(product_total.get(Price.class,"price").getValue() + product_tax_total.get(Price.class,"price").getValue());
        orderPrice.setCurrency("INR");
        quote.setTtl(15L*60L); //15 minutes.

        return items;
    }

    public void addItemsToQuote(Order order, LineItems line_items) {
        Items items = new Items();
        order.setItems(items);
        for (int i = 0 ;i < line_items.size() ; i ++ ){
            LineItem lineItem = line_items.get(i);
            createItemFromECommerceLineItem(items,lineItem);
        }
    }


    public in.succinct.bpp.shopify.helpers.model.Order confirmDraftOrder(Request request) {
        Order bapOrder = request.getMessage().getOrder();
        DraftOrder draftOrder = saveDraftOrder(request);
        //Update with latest attributes.
        JSONObject parameter=  new JSONObject();
        parameter.put("payment_pending",true);
        JSONObject response = putViaGet(String.format("/draft_orders/%s/complete.json",draftOrder.getId()),parameter);
        draftOrder = new DraftOrder((JSONObject) response.get("draft_order"));

        BecknOrderMeta orderMeta = Database.getTable(BecknOrderMeta.class).newRecord();
        orderMeta.setBecknTransactionId(request.getContext().getTransactionId());
        orderMeta = Database.getTable(BecknOrderMeta.class).getRefreshed(orderMeta);
        orderMeta.setECommerceOrderId(draftOrder.getOrderId());
        if (ObjectUtil.isVoid(orderMeta.getBapOrderId())){
            orderMeta.setBapOrderId(BecknIdHelper.getBecknId(orderMeta.getECommerceOrderId(),adaptor.getSubscriber().getSubscriberId(),Entity.order));
        }
        orderMeta.save();

        response = get(String.format("/orders/%s.json",draftOrder.getOrderId()),new JSONObject());


        in.succinct.bpp.shopify.helpers.model.Order order = new in.succinct.bpp.shopify.helpers.model.Order((JSONObject) response.get("order"));


        return order;
    }

    public DraftOrder saveDraftOrder(Request request) {
        Order bo = request.getMessage().getOrder();

        DraftOrder order = new DraftOrder();

        BecknOrderMeta orderMeta = Database.getTable(BecknOrderMeta.class).newRecord();
        orderMeta.setBecknTransactionId(request.getContext().getTransactionId());
        orderMeta = Database.getTable(BecknOrderMeta.class).getRefreshed(orderMeta);
        if (!orderMeta.getRawRecord().isNewRecord()){
            order.setId(orderMeta.getECommerceDraftOrderId());
        }

        if (!ObjectUtil.isVoid(bo.getId())){
            orderMeta.setBapOrderId(bo.getId());// Sent from bap.!
        }


        setBilling(order,bo.getBilling());

        setShipping(order,bo.getFulfillment());

        order.setEmail(bo.getFulfillment().getEnd().getContact().getEmail());
        order.setCurrency("INR");

        if (bo.getItems() != null ){

            LineItems line_items = new LineItems();
            order.setLineItems(line_items);

            bo.getItems().forEach(boItem->{
                in.succinct.bpp.search.db.model.Item dbItem = getItem(boItem.getId());
                Item refreshedBoItem = new Item(dbItem.getObjectJson());
                LineItem item = new LineItem();
                item.setVariantId(BecknIdHelper.getLocalUniqueId(boItem.getId(),Entity.item));
                item.setQuantity(boItem.getQuantity().getCount());
                item.setProductId(refreshedBoItem.getTags().get("product_id"));
                item.setRequiresShipping(true);
                item.setTaxable(doubleTypeConverter.valueOf(refreshedBoItem.getTags().get("tax_rate")) > 0);

                line_items.add(item);
            });
        }

        order.setSource("beckn");

        order.setName("beckn-"+request.getContext().getTransactionId());

        order.setNoteAttributes(new NoteAttributes());

        for (String key : new String[]{"bap_id","bap_uri","domain","transaction_id","city","country","core_version"}){
            Tag meta = new Tag();
            meta.setName(String.format("context.%s",key));
            meta.setValue(request.getContext().get(key));
            order.getNoteAttributes().add(meta);
        }

        if (!ObjectUtil.isVoid(order.getId())){
            delete(String.format("/draft_orders/%s.json",order.getId()) , new JSONObject());
            order.rm("id");
        }

        JSONObject outOrder =  post("/draft_orders.json", order.getInner());


        DraftOrder draftOrder = new DraftOrder((JSONObject) outOrder.get("draft_order"));

        orderMeta.setECommerceDraftOrderId(draftOrder.getId());
        orderMeta.save();

        return draftOrder;
    }

    private void setShipping(DraftOrder order, Fulfillment fulfillment){

        if (fulfillment == null){
            return;
        }
        User user = fulfillment.getCustomer();

        String[] parts = user.getPerson().getName().split(" ");
        DraftOrder.Address shipping = new DraftOrder.Address();
        order.setShippingAddress(shipping);

        shipping.setName(user.getPerson().getName());
        shipping.setFirstName(parts[0]);
        shipping.setLastName(user.getPerson().getName().substring(parts[0].length()));

        Address address = fulfillment.getEnd().getLocation().getAddress();
        Contact contact = fulfillment.getEnd().getContact();
        GeoCoordinate gps = fulfillment.getEnd().getLocation().getGps();
        Country country = Country.findByName(address.getCountry());
        State state = State.findByCountryAndName(country.getId(),address.getState());
        City city = City.findByStateAndName(state.getId(),address.getCity());

        shipping.setAddress1(address.getDoor()+"," +address.getBuilding());
        shipping.setAddress2(address.getStreet()+","+address.getLocality());
        shipping.setCity(city.getName());
        shipping.setProvinceCode(state.getCode());
        shipping.setProvince(state.getName());
        shipping.setZip(address.getAreaCode());
        shipping.setCountryCode(country.getIsoCode2());
        //shipping.put("email",contact.getEmail());
        shipping.setPhone(contact.getPhone());
        if (gps != null) {
            shipping.setLatitude(gps.getLat().doubleValue());
            shipping.setLongitude(gps.getLng().doubleValue());
        }
    }
    private void setBilling(DraftOrder draftOrder, Billing boBilling) {
        if (boBilling == null){
            return;
        }
        DraftOrder.Address billing = new DraftOrder.Address();
        draftOrder.setBillingAddress(billing);

        String[] parts = boBilling.getName().split(" ");
        billing.setName(boBilling.getName());
        billing.setFirstName(parts[0]);
        billing.setLastName(boBilling.getName().substring(parts[0].length()));
        if (boBilling.getAddress() != null){
            billing.setAddress1(boBilling.getAddress().getDoor()+"," +boBilling.getAddress().getBuilding());
            billing.setAddress2(boBilling.getAddress().getStreet()+","+boBilling.getAddress().getLocality());

            Country country = Country.findByName(boBilling.getAddress().getCountry());
            State state = State.findByCountryAndName(country.getId(),boBilling.getAddress().getState());
            City city = City.findByStateAndName(state.getId(),boBilling.getAddress().getCity());

            billing.setCity(city.getName());
            billing.setProvince(city.getState().getName());
            billing.setProvinceCode(city.getState().getCode());
            billing.setCountryCode(city.getState().getCountry().getIsoCode2());
            billing.setZip(boBilling.getAddress().getAreaCode());
        }


        billing.setPhone(boBilling.getPhone());

    }
    public Order getBecknOrder(DraftOrder eCommerceOrder) {

        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        if (eCommerceOrder instanceof in.succinct.bpp.shopify.helpers.model.Order) {
            meta.setECommerceOrderId(eCommerceOrder.getId());
        }else {
            meta.setECommerceDraftOrderId(eCommerceOrder.getId());
        }
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);


        Order order = new Order();
        order.setPayment(new Payment());
        setPayment(order.getPayment(),eCommerceOrder);

        Quote quote = new Quote();
        order.setQuote(quote);
        quote.setTtl(15*60);
        quote.setPrice(new Price());
        quote.getPrice().setValue(order.getPayment().getParams().getAmount());
        quote.getPrice().setCurrency(order.getPayment().getParams().getCurrency());
        quote.setBreakUp(new BreakUp());


        Price productPrice = new Price(); productPrice.setValue(eCommerceOrder.getSubtotalPrice());
        Price tax = new Price(); tax.setValue(eCommerceOrder.getTotalTax());
        Price total = new Price(); total.setValue(eCommerceOrder.getTotalPrice());
        Price shippingPrice = new Price(); shippingPrice.setValue(eCommerceOrder.getShippingLine().getPrice());

        BreakUpElement element = quote.getBreakUp().createElement("item","Total Product",productPrice);
        quote.getBreakUp().add(element);
        element = quote.getBreakUp().createElement("item","Total Tax",tax);
        quote.getBreakUp().add(element);

        element = quote.getBreakUp().createElement("item","Shipping",shippingPrice);
        quote.getBreakUp().add(element);

        element = quote.getBreakUp().createElement("item","Total",tax);
        quote.getBreakUp().add(element);



        //Delivery is included


        setBilling(order,eCommerceOrder.getBillingAddress());
        //order.setId(meta.getBapOrderId());
        order.setState(eCommerceOrder.getStatus());

        addItemsToQuote(order,eCommerceOrder.getLineItems());

        order.setFulfillment(new Fulfillment());
        order.getFulfillment().setEnd(new FulfillmentStop());
        order.getFulfillment().getEnd().setLocation(new Location());
        order.getFulfillment().getEnd().getLocation().setAddress(new Address());
        order.getFulfillment().getEnd().setContact(new Contact());
        order.getFulfillment().setCustomer(new User());
        order.getFulfillment().setState(order.getState());
        order.getFulfillment().setId(BecknIdHelper.getBecknId(eCommerceOrder.getId(),adaptor.getSubscriber().getSubscriberId(),Entity.fulfillment));

        Locations locations = getProviderLocations();
        if (locations.size() > 0) {
            order.getFulfillment().setStart(new FulfillmentStop());
            order.getFulfillment().getStart().setLocation(locations.get(0));
        }

        DraftOrder.Address shipping = eCommerceOrder.getShippingAddress();
        if (ObjectUtil.isVoid(shipping.getAddress1())){
            shipping = eCommerceOrder.getBillingAddress();
        }
        String[] address1_parts = (shipping.getAddress1()).split(",");
        String[] address2_parts = (shipping.getAddress2()).split(",");


        User user = order.getFulfillment().getCustomer();
        user.setPerson(new Person());
        Person person = user.getPerson();
        person.setName(shipping.getFirstName() + " " + shipping.getLastName());


        Address address = order.getFulfillment().getEnd().getLocation().getAddress();
        address.setDoor(address1_parts[0]);
        if (address1_parts.length > 1) {
            address.setBuilding(address1_parts[1]);
        }
        address.setStreet(address2_parts[0]);
        if (address2_parts.length > 1) {
            address.setLocality(address2_parts[1]);
        }
        Country country = Country.findByISO(shipping.getCountryCode());
        State state = State.findByCountryAndCode(country.getId(),shipping.getProvinceCode());
        City city = City.findByStateAndName(state.getId(),shipping.getCity());

        address.setCountry(country.getName());
        address.setState(state.getName());
        address.setPinCode(shipping.getZip());
        address.setCity(city.getName());

        order.getFulfillment().getEnd().getContact().setPhone(shipping.getPhone());
        order.getFulfillment().getEnd().getContact().setEmail(eCommerceOrder.getEmail());


        order.setProvider(new Provider());
        order.getProvider().setId(BecknIdHelper.getBecknId(adaptor.getSubscriber().getSubscriberId(),
                adaptor.getSubscriber().getSubscriberId(), Entity.provider));
        order.setProviderLocation(locations.get(0));
        if (!ObjectUtil.isVoid(meta.getBapOrderId())) {
            order.setId(meta.getBapOrderId());
        }

        return order;
    }


    private void createItemFromECommerceLineItem(Items items, LineItem eCommerceLineItem) {
        Item item = new Item();
        item.setDescriptor(new Descriptor());
        item.setId(BecknIdHelper.getBecknId(String.valueOf(eCommerceLineItem.getVariantId()), adaptor.getSubscriber().getSubscriberId(), Entity.item));
        item.getDescriptor().setName(eCommerceLineItem.getName());
        item.getDescriptor().setCode(eCommerceLineItem.getSku());
        if (ObjectUtil.isVoid(item.getDescriptor().getCode())){
            item.getDescriptor().setCode(item.getDescriptor().getName());
        }
        item.getDescriptor().setLongDesc(item.getDescriptor().getName());
        item.getDescriptor().setShortDesc(item.getDescriptor().getName());
        item.setQuantity(new Quantity());
        item.getQuantity().setCount(doubleTypeConverter.valueOf(eCommerceLineItem.getQuantity()).intValue());

        Price price = new Price();
        item.setPrice(price);

        //price.setListedValue(doubleTypeConverter.valueOf(eCommerceLineItem.get("subtotal")));
        price.setValue(doubleTypeConverter.valueOf(eCommerceLineItem.getPrice()));
        price.setCurrency("INR");
        items.add(item);


    }

    private boolean isPaid(PaymentTerms terms){
        if (terms == null){
            return false;
        }
        double toPay = terms.getAmount();

        PaymentSchedules schedules = terms.getPaymentSchedules();
        Bucket paid  = new Bucket();
        for (PaymentSchedule schedule : schedules){
            if (schedule.getCompletedAt() != null ) {
                paid.increment(schedule.getAmount());
            }
        }
        return DoubleUtils.compareTo(paid.doubleValue(), toPay)>= 0;
    }



    private void setPayment(Payment payment, DraftOrder eCommerceOrder) {
        if (!isPaid(eCommerceOrder.getPaymentTerms())) {
            payment.setStatus("NOT-PAID");
        }else {
            payment.setStatus("PAID");
        }
        payment.setType("POST-FULFILLMENT");
        payment.setParams(new Params());
        payment.getParams().setCurrency(getStore().getCurrency());
        if (eCommerceOrder.getPaymentTerms() != null) {
            payment.getParams().setAmount(doubleTypeConverter.valueOf(eCommerceOrder.getPaymentTerms().getAmount()));
        }
    }

    private void setBilling(Order order, DraftOrder.Address billingAddress) {
        Billing billing = new Billing();
        order.setBilling(billing);

        Address address = new Address();
        billing.setAddress(address);

        billing.setName(billingAddress.getFirstName() + " " + billingAddress.getLastName());
        billing.setPhone((String) billingAddress.getPhone());

        address.setName(billing.getName());
        address.setStreet(billingAddress.getAddress1());
        address.setLocality(billingAddress.getAddress2());
        address.setPinCode(billingAddress.getZip());

        Country country= Country.findByISO(billingAddress.getCountryCode());
        State state = State.findByCountryAndCode(country.getId(),billingAddress.getProvince());
        City city = City.findByStateAndName(state.getId(),billingAddress.getCity());

        address.setCountry(country.getName());
        address.setState(state.getName());
        address.setCity(city.getName());

    }



    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T putViaPost(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<String>(){{
            put("X-HTTP-Method-Override","PUT");
        }});
    }
    public <T extends JSONAware> T putViaGet(String relativeUrl, JSONObject parameter ){
        return get(relativeUrl,parameter,new IgnoreCaseMap<String>(){{
            put("X-HTTP-Method-Override","PUT");
        }});
    }
    public <T extends JSONAware> T delete(String relativeUrl, JSONObject parameter ){
        return get(relativeUrl,parameter,new IgnoreCaseMap<>(){{
            put("X-HTTP-Method-Override","DELETE");
        }});
    }
    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter , Map<String,String> addnlHeaders){
        return new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .inputFormat(InputFormat.JSON).input(parameter).method(HttpMethod.POST).getResponseAsJson();
    }
    public <T extends JSONAware> T get(String relativeUrl, JSONObject parameter){
        return get(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T get(String relativeUrl, JSONObject parameter, Map<String,String> addnlHeaders){
        return new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .input(parameter)
                .method(HttpMethod.GET).getResponseAsJson();
    }

    public <T extends JSONAware> Page<T> getPage(String relativeUrl, JSONObject parameter){
        return getPage(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> Page<T> getPage(String relativeUrl, JSONObject parameter, Map<String,String> addnlHeaders){
        Call<JSONObject> call = new Call<JSONObject>().url(getAdminApiUrl(),relativeUrl).header("content-type", MimeType.APPLICATION_JSON.toString()).header("X-Shopify-Access-Token",getAccessToken()).headers(addnlHeaders)
                .input(parameter)
                .method(HttpMethod.GET);

        Page<T> page = new Page<>();
        page.data = call.getResponseAsJson();
        List<String> links = call.getResponseHeaders().get("Link");
        if (!links.isEmpty()){
            String link = links.get(0);
            Matcher matcher = Pattern.compile("(<)(https://[^\\?]*\\?page_info=[^&]*&limit=[0-9]*)(>; rel=)(previous|next)([ ,])*").matcher(link);
            matcher.results().forEach(mr->{
                if (mr.group(4).equals("next")) {
                    page.next = mr.group(2);
                }else {
                    page.previous = mr.group(2);
                }
            });
        }


        return page;
    }

    public static class Page<T extends JSONAware> {
        T data;
        String next = null;
        String previous = null;
    }
}
