package in.succinct.bpp.shopify.adaptor;

import com.venky.cache.Cache;
import com.venky.core.math.DoubleHolder;
import com.venky.core.math.DoubleUtils;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
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
import in.succinct.beckn.BreakUp.BreakUpElement.BreakUpCategory;
import in.succinct.beckn.Contact;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.FulfillmentType;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Images;
import in.succinct.beckn.Item;
import in.succinct.beckn.Item.PackagedCommodity;
import in.succinct.beckn.ItemQuantity;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Order;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payment.CollectedBy;
import in.succinct.beckn.Payment.CommissionType;
import in.succinct.beckn.Payment.Params;
import in.succinct.beckn.Payment.PaymentStatus;
import in.succinct.beckn.Payment.PaymentType;
import in.succinct.beckn.Payment.SettlementBasis;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Person;
import in.succinct.beckn.Price;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.beckn.SettlementDetail;
import in.succinct.beckn.SettlementDetail.SettlementCounterparty;
import in.succinct.beckn.SettlementDetail.SettlementPhase;
import in.succinct.beckn.SettlementDetail.SettlementType;
import in.succinct.beckn.SettlementDetails;
import in.succinct.beckn.Tag;
import in.succinct.beckn.Tags;
import in.succinct.beckn.User;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.TimeSensitiveCache;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper;
import in.succinct.bpp.core.adaptor.api.BecknIdHelper.Entity;
import in.succinct.bpp.core.db.model.BecknOrderMeta;
import in.succinct.bpp.search.adaptor.SearchAdaptor;
import in.succinct.bpp.search.db.model.ProviderLocation;
import in.succinct.bpp.shopify.adaptor.ECommerceSDK.Page;
import in.succinct.bpp.shopify.model.DraftOrder;
import in.succinct.bpp.shopify.model.DraftOrder.LineItem;
import in.succinct.bpp.shopify.model.DraftOrder.LineItems;
import in.succinct.bpp.shopify.model.DraftOrder.NoteAttributes;
import in.succinct.bpp.shopify.model.DraftOrder.PaymentSchedule;
import in.succinct.bpp.shopify.model.DraftOrder.PaymentSchedules;
import in.succinct.bpp.shopify.model.DraftOrder.PaymentTerms;
import in.succinct.bpp.shopify.model.Order.Fulfillments;
import in.succinct.bpp.shopify.model.ProductImages;
import in.succinct.bpp.shopify.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.model.Products;
import in.succinct.bpp.shopify.model.Products.InventoryItem;
import in.succinct.bpp.shopify.model.Products.InventoryItems;
import in.succinct.bpp.shopify.model.Products.InventoryLevel;
import in.succinct.bpp.shopify.model.Products.InventoryLevels;
import in.succinct.bpp.shopify.model.Products.Metafield;
import in.succinct.bpp.shopify.model.Products.Product;
import in.succinct.bpp.shopify.model.Products.ProductVariant;
import in.succinct.bpp.shopify.model.Store;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class ECommerceAdaptor extends CommerceAdaptor {
    final ECommerceSDK helper;
    final TimeSensitiveCache cache = new TimeSensitiveCache(Duration.ofSeconds(60));

    public ECommerceAdaptor(Map<String, String> configuration, Subscriber subscriber) {
        super(configuration, subscriber);
        this.helper = new ECommerceSDK(this);
        getProviderConfig().getSupportContact().setEmail(getSupportEmail());
    }


    public Order initializeDraftOrder(Request request) {
        DraftOrder draftOrder = new DraftOrder();
        Order bo = request.getMessage().getOrder();

        BecknOrderMeta orderMeta = Database.getTable(BecknOrderMeta.class).newRecord();
        orderMeta.setBecknTransactionId(request.getContext().getTransactionId());
        orderMeta = Database.getTable(BecknOrderMeta.class).getRefreshed(orderMeta);


        if (!ObjectUtil.isVoid(bo.getId())) {
            orderMeta.setBapOrderId(bo.getId());// Sent from bap.!
        }

        if (!orderMeta.getRawRecord().isNewRecord() && !ObjectUtil.isVoid(orderMeta.getECommerceDraftOrderId())) {
            draftOrder.setId(orderMeta.getECommerceDraftOrderId());
        }
        draftOrder.setEmail(bo.getFulfillment().getEnd().getContact().getEmail());
        draftOrder.setCurrency("INR");
        draftOrder.setSource("beckn");
        draftOrder.setName("beckn-" + request.getContext().getTransactionId());
        draftOrder.setNoteAttributes(new NoteAttributes());

        for (String key : new String[]{"bap_id", "bap_uri", "domain", "transaction_id", "city", "country", "core_version"}) {
            Tag meta = new Tag();
            meta.setName(String.format("context.%s", key));
            meta.setValue(request.getContext().get(key));
            draftOrder.getNoteAttributes().add(meta);
        }

        if (!ObjectUtil.isVoid(draftOrder.getId())) {
            delete(draftOrder);
        }

        Order lastKnown = new Order(orderMeta.getOrderJson());
        lastKnown.setFulfillments(bo.getFulfillments());
        lastKnown.setProviderLocation(bo.getProviderLocation());
        lastKnown.setItems(bo.getItems());
        if (lastKnown.getFulfillments().size() > 0) {
            lastKnown.setFulfillment(lastKnown.getFulfillments().get(0));
        }
        lastKnown.setBilling(bo.getBilling());
        lastKnown.setPayment(bo.getPayment());
        orderMeta.setOrderJson(lastKnown.toString());


        setBilling( bo.getBilling(),draftOrder);
        setShipping( bo.getFulfillment(), draftOrder);

        if (bo.getItems() != null) {
            bo.getItems().forEach(boItem -> {
                in.succinct.bpp.search.db.model.Item dbItem = getItem(boItem.getId());
                Item refreshedBoItem = new Item(dbItem.getObjectJson());
                refreshedBoItem.setItemQuantity(boItem.getItemQuantity());
                addItem(draftOrder, refreshedBoItem);
            });
        }

        return saveDraftOrder(draftOrder, orderMeta);
    }

    private void delete(DraftOrder draftOrder) {
        helper.delete(String.format("/draft_orders/%s.json",draftOrder.getId()) , new JSONObject());
        draftOrder.rm("id");
    }

    private in.succinct.bpp.search.db.model.Item getItem(String objectId) {

        Select select = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        List<in.succinct.bpp.search.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "APPLICATION_ID", Operator.EQ, getApplication().getId())).
                add(new Expression(select.getPool(), "OBJECT_ID", Operator.EQ, objectId))).execute(1);

        in.succinct.bpp.search.db.model.Item dbItem = dbItems.isEmpty() ? null : dbItems.get(0);
        return dbItem;
    }

    private void addItem(DraftOrder draftOrder, Item item) {
        LineItems line_items = draftOrder.getLineItems();
        if (line_items == null) {
            line_items = new LineItems();
            draftOrder.setLineItems(line_items);
        }
        LineItem lineItem = new LineItem();
        lineItem.setVariantId(BecknIdHelper.getLocalUniqueId(item.getId(), BecknIdHelper.Entity.item));
        lineItem.setQuantity(item.getItemQuantity().getSelected().getCount());
        lineItem.setProductId(item.getTags().get("product_id"));
        lineItem.setRequiresShipping(true);
        lineItem.setTaxable(doubleTypeConverter.valueOf(item.getTags().get("tax_rate")) > 0);
        line_items.add(lineItem);

    }

    private TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private TypeConverter<Boolean> booleanTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();

    public void setBilling(Billing source,DraftOrder target) {

        if (source == null) {
            return;
        }
        DraftOrder.Address billing = new DraftOrder.Address();
        target.setBillingAddress(billing);

        String[] parts = source.getName().split(" ");
        billing.setName(source.getName());
        billing.setFirstName(parts[0]);
        billing.setLastName(source.getName().substring(parts[0].length()));
        if (source.getAddress() != null) {
            billing.setAddress1(source.getAddress().getDoor() + "," + source.getAddress().getBuilding());
            billing.setAddress2(source.getAddress().getStreet() + "," + source.getAddress().getLocality());

            Country country = Country.findByName(source.getAddress().getCountry());
            State state = State.findByCountryAndName(country.getId(), source.getAddress().getState());
            City city = City.findByStateAndName(state.getId(), source.getAddress().getCity());

            billing.setCity(city.getName());
            billing.setProvince(city.getState().getName());
            billing.setProvinceCode(city.getState().getCode());
            billing.setCountryCode(city.getState().getCountry().getIsoCode2());
            billing.setZip(source.getAddress().getAreaCode());
        }


        billing.setPhone(source.getPhone());

    }

    public void setShipping(Fulfillment source,DraftOrder target) {
        if (source == null) {
            return;
        }
        User user = source.getCustomer();

        String[] parts = user.getPerson().getName().split(" ");
        DraftOrder.Address shipping = new DraftOrder.Address();
        target.setShippingAddress(shipping);

        shipping.setName(user.getPerson().getName());
        shipping.setFirstName(parts[0]);
        shipping.setLastName(user.getPerson().getName().substring(parts[0].length()));

        Address address = source.getEnd().getLocation().getAddress();
        Contact contact = source.getEnd().getContact();
        GeoCoordinate gps = source.getEnd().getLocation().getGps();
        Country country = Country.findByName(address.getCountry());
        State state = State.findByCountryAndName(country.getId(), address.getState());
        City city = City.findByStateAndName(state.getId(), address.getCity());

        shipping.setAddress1(address.getDoor() + "," + address.getBuilding());
        shipping.setAddress2(address.getStreet() + "," + address.getLocality());
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

    public Order saveDraftOrder(DraftOrder draftOrder, BecknOrderMeta orderMeta) {

        JSONObject outOrder = helper.post("/draft_orders.json", draftOrder.getInner());
        DraftOrder oDraftOrder = new DraftOrder((JSONObject) outOrder.get("draft_order"));

        orderMeta.setECommerceDraftOrderId(oDraftOrder.getId());
        orderMeta.save();

        return getBecknOrder(draftOrder,orderMeta);

    }

    public Order confirmDraftOrder(Order inOrder) {

        BecknOrderMeta orderMeta = Database.getTable(BecknOrderMeta.class).newRecord();
        orderMeta.setECommerceDraftOrderId(inOrder.getId());
        orderMeta = Database.getTable(BecknOrderMeta.class).getRefreshed(orderMeta);

        //Update with latest attributes.
        JSONObject parameter = new JSONObject();
        parameter.put("payment_pending", inOrder.getPayment().getStatus() != PaymentStatus.PAID);
        JSONObject response = helper.putViaGet(String.format("/draft_orders/%s/complete.json", orderMeta.getECommerceDraftOrderId()), parameter); //Marks payment as paid!

        DraftOrder draftOrder = new DraftOrder((JSONObject) response.get("draft_order"));

        orderMeta.setECommerceOrderId(draftOrder.getOrderId());
        if (ObjectUtil.isVoid(orderMeta.getBapOrderId())) {
            orderMeta.setBapOrderId(BecknIdHelper.getBecknId(orderMeta.getECommerceOrderId(), getSubscriber().getSubscriberId(), Entity.order));
        }
        orderMeta.save();

        response = helper.get(String.format("/orders/%s.json", draftOrder.getOrderId()), new JSONObject());


        in.succinct.bpp.shopify.model.Order order = new in.succinct.bpp.shopify.model.Order((JSONObject) response.get("order"));


        return getBecknOrder(order);
    }


    @Override
    public String getTrackingUrl(Order order) {
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);


        JSONObject params = new JSONObject();
        params.put("fields", "tracking_urls");

        JSONObject fulfillmentJson = helper.get(String.format("/orders/%s/fulfillments.json", meta.getECommerceOrderId()), params);
        Fulfillments fulfillments = new Fulfillments((JSONArray) fulfillmentJson.get("fulfillments"));
        String url = fulfillments.get(0).getTrackingUrls().get(0);

        return url;
    }

    @Override
    public Order cancel(Order order) {
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        JSONObject params = new JSONObject();
        params.put("reason", "customer");


        JSONObject response = helper.post(String.format("/orders/%s/cancel.json", meta.getECommerceOrderId()), params);
        in.succinct.bpp.shopify.model.Order eCommerceOrder = new in.succinct.bpp.shopify.model.Order((JSONObject) response.get("order"));
        Order outOrder = getBecknOrder(eCommerceOrder);
        return outOrder;
    }

    @Override
    public Order getStatus(Order order) {
        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBapOrderId(order.getId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        /* Take status message and fill response with on_status message */
        JSONObject response = helper.get(String.format("/orders/%s.json", meta.getECommerceOrderId()), new JSONObject());
        in.succinct.bpp.shopify.model.Order eCommerceOrder = new in.succinct.bpp.shopify.model.Order((JSONObject) response.get("order"));

        return getBecknOrder(eCommerceOrder);
    }


    public String getSupportEmail() {
        return getStore().getSupportEmail();
    }


    @Override
    public Locations getProviderLocations() {
        return cache.get(Locations.class, () -> {
            Locations locations = new Locations();
            Store onlineStore = getStore();

            JSONObject response = helper.get("/locations.json", new JSONObject());
            JSONArray stores = (JSONArray) response.get("locations");
            stores.forEach(o -> {
                JSONObject store = (JSONObject) o;

                if (!booleanTypeConverter.valueOf(store.get("active")) || !booleanTypeConverter.valueOf(store.get("legacy"))) {
                    return;
                }
                if (!ObjectUtil.equals(onlineStore.getPrimaryLocationId() ,store.get("id"))){
                    return;
                }
                Location location = new Location();
                location.setId(BecknIdHelper.getBecknId(StringUtil.valueOf(store.get("id")), getSubscriber().getSubscriberId(), Entity.provider_location));
                location.setAddress(new Address());
                location.getAddress().setName((String) store.get("name"));
                location.getAddress().setStreet((String)store.get("address1"));
                location.getAddress().setLocality((String)store.get("address2"));
                location.getAddress().setCity((String) store.get("city"));
                location.getAddress().setPinCode((String) store.get("zip"));
                location.getAddress().setCountry((String) store.get("country"));
                location.getAddress().setState((String) store.get("province"));
                location.setTime(getProviderConfig().getTime());

                locations.add(location);
            });
            return locations;
        });
    }

    @Override
    public Items getItems() {
        return cache.get(Items.class, () -> {
            Items items = new Items();

            InventoryLevels levels = getInventoryLevels();
            Cache<Long, List<Long>> inventoryLocations = new Cache<>(0, 0) {
                @Override
                protected List<Long> getValue(Long aLong) {
                    return new ArrayList<>();
                }
            };
            levels.forEach(level -> inventoryLocations.get(level.getInventoryItemId()).add(level.getLocationId()));

            InventoryItems inventoryItems = getInventoryItems(inventoryLocations.keySet());

            JSONObject input = new JSONObject();
            input.put("limit", 250);
            Page<JSONObject> productsPage = new Page<>();
            productsPage.next = "/products.json";

            Map<String, Double> taxRateMap = getTaxRateMap();

            while (productsPage.next != null) {
                productsPage = helper.getPage(productsPage.next, input);
                Products products = new Products((JSONArray) productsPage.data.get("products"));
                for (Product product : products) {
                    if (!product.isActive()) {
                        continue;
                    }

                    for (ProductVariant variant : product.getProductVariants()) {
                        inventoryLocations.get(variant.getInventoryItemId()).forEach(location_id -> {
                            Item item = new Item();
                            item.setId(BecknIdHelper.getBecknId(variant.getId(), getSubscriber().getSubscriberId(), Entity.item));

                            Descriptor descriptor = new Descriptor();
                            item.setDescriptor(descriptor);
                            descriptor.setCode(variant.getBarCode());
                            descriptor.setShortDesc(variant.getTitle());
                            descriptor.setLongDesc(variant.getTitle());
                            descriptor.setImages(new Images());
                            ProductImages productImages = product.getImages();

                            if (variant.getImageId() > 0) {
                                ProductImage image = productImages.get(StringUtil.valueOf(variant.getImageId()));
                                if (image != null) {
                                    descriptor.getImages().add(image.getSrc());
                                }
                            } else if (productImages.size() > 0) {
                                for (ProductImage i : productImages) {
                                    descriptor.getImages().add(i.getSrc());
                                }
                            }
                            descriptor.setSymbol(descriptor.getImages().get(0));

                            item.setCategoryId(getProviderConfig().getCategory().getId());
                            item.setCategoryIds(new BecknStrings());
                            item.getCategoryIds().add(item.getCategoryId());
                            item.setTags(new Tags());
                            StringTokenizer tokenizer = new StringTokenizer(product.getTags(), ",");
                            while (tokenizer.hasMoreTokens()) {
                                item.getTags().set(tokenizer.nextToken(), "true");
                            }
                            InventoryItem inventoryItem = inventoryItems.get(StringUtil.valueOf(variant.getInventoryItemId()));
                            item.getTags().set("hsn_code", inventoryItem.getHarmonizedSystemCode());
                            item.getTags().set("country_of_origin", inventoryItem.getCountryCodeOfOrigin());
                            item.getTags().set("tax_rate", taxRateMap.get(inventoryItem.getHarmonizedSystemCode()));
                            item.getTags().set("product_id", variant.getProductId());
                            if (product.isVeg()) {
                                item.getTags().set("veg", "yes");
                                item.getTags().set("non-veg", "no");
                            }else {
                                item.getTags().set("veg", "no");
                                item.getTags().set("non-veg", "yes");
                            }



                            item.setLocationId(BecknIdHelper.getBecknId(StringUtil.valueOf(location_id), getSubscriber().getSubscriberId(), Entity.provider_location));
                            item.setLocationIds(new BecknStrings());
                            item.getLocationIds().add(item.getLocationId());



                            Price price = new Price();
                            item.setPrice(price);
                            price.setMaximumValue(variant.getMrp());
                            price.setListedValue(variant.getPrice());
                            price.setCurrency(getStore().getCurrency());
                            price.setValue(variant.getPrice());
                            price.setOfferedValue(variant.getPrice());


                            item.setPaymentIds(new BecknStrings());
                            item.getPaymentIds().add(BecknIdHelper.getBecknId("1", getSubscriber().getSubscriberId(), Entity.payment)); //Only allow By BAP , ON_ORDER

                            item.setReturnable(getProviderConfig().isReturnSupported());
                            if (item.isReturnable()){
                                item.setReturnWindow(getProviderConfig().getReturnWindow());
                                item.setSellerPickupReturn(getProviderConfig().isReturnPickupSupported());
                            }
                            item.setCancellable(true);
                            item.setTimeToShip(getProviderConfig().getTurnAroundTime());
                            item.setAvailableOnCod(getProviderConfig().isCodSupported());

                            item.setContactDetailsConsumerCare(getProviderConfig().getLocation().getAddress().flatten() + " " + getProviderConfig().getSupportContact().flatten());

                            /*
                            PackagedCommodity packagedCommodity = new PackagedCommodity();

                            item.setPackagedCommodity(packagedCommodity);
                            packagedCommodity.setManufacturerOfPackerAddress(item.getContactDetailsConsumerCare());
                            packagedCommodity.setManufacturerOfPackerName(getProviderConfig().getStoreName());
                            packagedCommodity.setCommonOrGenericNameOfCommodity(product.getProductType());
                            packagedCommodity.setNetQuantityOrMeasureOfCommodityInPkg(variant.getGrams() + " Grams");
                            */

                            //For statutory requirements show images.

                            items.add(item);

                        });
                    }
                }
            }
            return items;
        });
    }

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


    private InventoryLevels getInventoryLevels() {
        return cache.get(InventoryLevels.class,()->{
            StringBuilder locationIds = new StringBuilder();
            for (Iterator<Location> i = getProviderLocations().iterator(); i.hasNext(); ) {
                Location location = i.next();
                locationIds.append(location.getId());
                if (i.hasNext()) {
                    locationIds.append(",");
                }
            }
            JSONObject input = new JSONObject();
            input.put("location_ids", locationIds.toString());
            input.put("limit", 250);

            InventoryLevels levels = new InventoryLevels();
            Page<JSONObject> pageInventoryLevels = new Page<>();
            pageInventoryLevels.next = "/inventory_levels";
            while (pageInventoryLevels.next != null) {
                pageInventoryLevels = helper.getPage(pageInventoryLevels.next, input);
                JSONArray inventoryLevels = (JSONArray) pageInventoryLevels.data.get("inventory_levels");
                inventoryLevels.forEach(o -> {
                    levels.add(new InventoryLevel((JSONObject) o));
                });
                input.remove("location_ids");
            }
            return levels;
        });
    }

    private InventoryItems getInventoryItems(Set<Long> ids){
        return cache.get(InventoryItems.class,()->{
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
                pageInventoryItems = helper.getPage(pageInventoryItems.next,input);
                JSONArray inventoryItemsArray = (JSONArray) pageInventoryItems.data.get("inventory_items");
                inventoryItemsArray.forEach(o->{
                    inventoryItems.add(new InventoryItem((JSONObject)o));
                });
                input.remove("ids");
            }
            return inventoryItems;
        });
    }


    public Store getStore(){
        return cache.get(Store.class,()->{
            JSONObject storeJson = helper.get("/shop.json", new JSONObject());
            return new Store((JSONObject) storeJson.get("store"));
        });
    }



    @Override
    public boolean isTaxIncludedInPrice() {
        return getStore().isTaxesIncluded();
    }


    //Get Beckn Order

    public Order getBecknOrder(DraftOrder eCommerceOrder) {

        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        if (eCommerceOrder instanceof in.succinct.bpp.shopify.model.Order) {
            meta.setECommerceOrderId(eCommerceOrder.getId());
        }else {
            meta.setECommerceDraftOrderId(eCommerceOrder.getId());
        }
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);

        return getBecknOrder(eCommerceOrder,meta);
    }
    public Order getBecknOrder(DraftOrder eCommerceOrder,BecknOrderMeta meta) {

        Order lastReturnedOrderJson = new Order(meta.getOrderJson());

        Order order = new Order();
        order.setPayment(new Payment());
        setPayment(order.getPayment(),eCommerceOrder);
        order.getPayment().setBuyerAppFinderFeeAmount(lastReturnedOrderJson.getPayment().getBuyerAppFinderFeeAmount());
        order.getPayment().setBuyerAppFinderFeeType(lastReturnedOrderJson.getPayment().getBuyerAppFinderFeeType());
        if (order.getPayment().getBuyerAppFinderFeeType() == CommissionType.Percent){
            if (order.getPayment().getBuyerAppFinderFeeAmount() > getProviderConfig().getMaxAllowedCommissionPercent()){
                throw new RuntimeException("Max commission percent exceeded");
            }
        }else {
            double pct = new DoubleHolder(100.0 * order.getPayment().getBuyerAppFinderFeeAmount()/order.getPayment().getParams().getAmount(),2).getHeldDouble().doubleValue();
            if (pct > getProviderConfig().getMaxAllowedCommissionPercent()){
                throw new RuntimeException("Max commission percent exceeded");
            }
        }

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

        /* BreakUpElement element = quote.getBreakUp().createElement(BreakUpCategory.item,"Total Product",productPrice);
        quote.getBreakUp().add(element);

         */
        BreakUpElement element = quote.getBreakUp().createElement(BreakUpCategory.tax,"Tax",tax);
        quote.getBreakUp().add(element);

        element = quote.getBreakUp().createElement(BreakUpCategory.delivery,"Delivery Charges",shippingPrice);
        quote.getBreakUp().add(element);

        /* element = quote.getBreakUp().createElement(BreakUpCategory.item,"Total",total);
        quote.getBreakUp().add(element);
        */


        //Delivery is included


        setBilling(order,eCommerceOrder.getBillingAddress());
        //order.setId(meta.getBapOrderId());
        order.setState(eCommerceOrder.getStatus());

        order.setItems(new Items());

        eCommerceOrder.getLineItems().forEach(lineItem -> {
            Item item = createItemFromECommerceLineItem(lineItem);
            item.setFulfillmentId(lastReturnedOrderJson.getFulfillment().getId());
            BreakUpElement itemPrice = quote.getBreakUp().createElement(BreakUpCategory.item,"item",item.getPrice()); //This is line price and not unit price.
            itemPrice.setItemQuantity(item.getQuantity());
            itemPrice.setItem(item);
            itemPrice.setItemId(item.getId());

            quote.getBreakUp().add(itemPrice);
            order.getItems().add(item);
        });

        DraftOrder.Address shipping = eCommerceOrder.getShippingAddress();
        if (ObjectUtil.isVoid(shipping.getAddress1())){
            shipping = eCommerceOrder.getBillingAddress();
        }
        String[] address1_parts = (shipping.getAddress1()).split(",");
        String[] address2_parts = (shipping.getAddress2()).split(",");

        Locations locations = getProviderLocations();
        Location providerLocation = locations.get(lastReturnedOrderJson.getProviderLocation().getId());
        order.setProviderLocation(providerLocation);

        order.setFulfillment(new Fulfillment());
        order.getFulfillment().setId(lastReturnedOrderJson.getFulfillment().getId());
        order.getFulfillment().setStart(new FulfillmentStop());
        order.getFulfillment().getStart().setLocation(providerLocation);

        order.getFulfillment().setCustomer(new User());
        User user = order.getFulfillment().getCustomer();
        user.setPerson(new Person());
        Person person = user.getPerson();
        person.setName(shipping.getFirstName() + " " + shipping.getLastName());

        order.getFulfillment().setState(order.getState());
        order.getFulfillment().setEnd(new FulfillmentStop());
        order.getFulfillment().getEnd().setLocation(new Location());
        order.getFulfillment().getEnd().getLocation().setAddress(new Address());
        order.getFulfillment().getEnd().setContact(new Contact());
        order.getFulfillment().getEnd().getContact().setPhone(shipping.getPhone());
        order.getFulfillment().getEnd().getContact().setEmail(eCommerceOrder.getEmail());

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

        order.setProvider(new Provider());
        order.getProvider().setId(BecknIdHelper.getBecknId(getSubscriber().getSubscriberId(),
                getSubscriber().getSubscriberId(), Entity.provider));
        if (!ObjectUtil.isVoid(meta.getBapOrderId())) {
            order.setId(meta.getBapOrderId());
        }

        meta.setOrderJson(order.toString());
        meta.save();


        return order;
    }

    private Item createItemFromECommerceLineItem(LineItem eCommerceLineItem) {
        Item item = new Item();
        item.setDescriptor(new Descriptor());
        item.setId(BecknIdHelper.getBecknId(String.valueOf(eCommerceLineItem.getVariantId()), getSubscriber().getSubscriberId(), Entity.item));
        item.getDescriptor().setName(eCommerceLineItem.getName());
        item.getDescriptor().setCode(eCommerceLineItem.getSku());
        if (ObjectUtil.isVoid(item.getDescriptor().getCode())){
            item.getDescriptor().setCode(item.getDescriptor().getName());
        }
        item.getDescriptor().setLongDesc(item.getDescriptor().getName());
        item.getDescriptor().setShortDesc(item.getDescriptor().getName());
        item.setItemQuantity(new ItemQuantity());
        item.getItemQuantity().setAllocated(new Quantity());
        item.getItemQuantity().getAllocated().setCount(doubleTypeConverter.valueOf(eCommerceLineItem.getQuantity()).intValue());

        Price price = new Price();
        item.setPrice(price);

        //price.setListedValue(doubleTypeConverter.valueOf(eCommerceLineItem.get("subtotal")));
        price.setValue(doubleTypeConverter.valueOf(eCommerceLineItem.getPrice()));
        price.setCurrency("INR");


        return item;

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
        Payments payments =getSupportedPaymentCollectionMethods();
        if (payments.size() == 1){
            payment.setType(payments.get(0).getType());
        }
        if (!isPaid(eCommerceOrder.getPaymentTerms())) {
            payment.setStatus(PaymentStatus.NOT_PAID);
        }else {
            payment.setStatus(PaymentStatus.PAID);
        }
        payment.setSettlementBasis(SettlementBasis.Collection);
        payment.setCollectedBy(CollectedBy.BAP);
        payment.setSettlementDetails(new SettlementDetails());
        SettlementDetail detail = new SettlementDetail();
        payment.getSettlementDetails().add(detail);
        detail.setUpiAddress(getProviderConfig().getVPA());
        detail.setSettlementType(SettlementType.UPI);
        detail.setSettlementPhase(SettlementPhase.SALE_AMOUNT);
        detail.setSettlementCounterparty(SettlementCounterparty.SELLER_APP);

        payment.setParams(new Params());
        payment.getParams().setCurrency(getStore().getCurrency());
        if (eCommerceOrder.getPaymentTerms() != null) {
            payment.getParams().setAmount(doubleTypeConverter.valueOf(eCommerceOrder.getPaymentTerms().getAmount()));
        }

    }

    private void setBilling(Order target, DraftOrder.Address source) {
        Billing billing = new Billing();
        target.setBilling(billing);

        Address address = new Address();
        billing.setAddress(address);

        billing.setName(source.getFirstName() + " " + source.getLastName());
        billing.setPhone(source.getPhone());

        address.setName(billing.getName());
        address.setStreet(source.getAddress1());
        address.setLocality(source.getAddress2());
        address.setPinCode(source.getZip());

        Country country= Country.findByISO(source.getCountryCode());
        State state = State.findByCountryAndCode(country.getId(),source.getProvince());
        City city = City.findByStateAndName(state.getId(),source.getCity());

        address.setCountry(country.getName());
        address.setState(state.getName());
        address.setCity(city.getName());

    }

}
