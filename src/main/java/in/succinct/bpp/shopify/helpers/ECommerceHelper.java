package in.succinct.bpp.shopify.helpers;

import com.venky.cache.Cache;
import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.db.JdbcTypeHelper.TypeConverter;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.collab.db.model.config.Country;
import com.venky.swf.plugins.collab.db.model.config.State;
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
import in.succinct.beckn.Tags;
import in.succinct.beckn.User;
import in.succinct.bpp.shopify.adaptor.ECommerceAdaptor;
import in.succinct.bpp.shopify.helpers.BecknIdHelper.Entity;
import in.succinct.bpp.shopify.helpers.model.ProductImages;
import in.succinct.bpp.shopify.helpers.model.ProductImages.ProductImage;
import in.succinct.bpp.shopify.helpers.model.Products;
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
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ECommerceHelper {
    final ECommerceAdaptor adaptor;
    public ECommerceHelper(ECommerceAdaptor adaptor){
        this.adaptor = adaptor;
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
        /*
        InventoryItems inventoryItems = getInventoryItems(inventoryLocations.keySet());
        */
        JSONObject input = new JSONObject();
        input.put("limit",250);
        Page<JSONObject> productsPage = new Page<>();
        productsPage.next="/products.json";

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
                        item.setId(BecknIdHelper.getBecknId(String.valueOf(variant.getInventoryItemId()),adaptor.getSubscriber().getSubscriberId(),Entity.item));

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
    private Store getStore(){
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

    public Quote createQuote(Order order){
        Quote quote = new Quote();
        order.setQuote(quote);
        return quote;
    }

    public boolean isTaxIncludedInPrice(){
        return getStore().getTaxesIncluded();
    }


    private TypeConverter<Double> doubleTypeConverter = Database.getJdbcTypeHelper("").getTypeRef(double.class).getTypeConverter();
    private TypeConverter<Boolean> booleanTypeConverter  = Database.getJdbcTypeHelper("").getTypeRef(boolean.class).getTypeConverter();


    public Items loadItems(Order order, Items inItems) {
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

        Map<String,Double> taxRateMap = new Cache<String, Double>() {
            @Override
            protected Double getValue(String taxClass) {
                if (!ObjectUtil.isVoid(taxClass)){
                    JSONObject search_params = new JSONObject();
                    search_params.put("class",taxClass);
                    JSONArray taxRates = ECommerceHelper.this.get("/taxes",search_params);
                    if (taxRates.size() > 0 ){
                        JSONObject taxRate = (JSONObject) taxRates.get(0);
                        return doubleTypeConverter.valueOf(taxRate.get("rate"));
                    }
                }
                return 0.0D;
            }
        };



        for (int i = 0 ; i < inItems.size() ; i ++ ){
            Item inItem = inItems.get(i);
            Item outItem = new Item();
            outItem.setId(inItem.getId());

            String localId = BecknIdHelper.getLocalUniqueId(inItem.getId(), Entity.item);

            Quantity quantity = inItem.get(Quantity.class,"quantity");


            QuantitySummary outQuantity = new QuantitySummary();
            outItem.set("quantity",outQuantity);
            outQuantity.setSelected(quantity);

            JSONObject inventory = get("/products/"+localId,new JSONObject());

            if (inventory == null ){
                throw new RuntimeException("No inventory with provider.");
            }

            String taxClass = (String)inventory.get("tax_class");
            double taxRate = taxRateMap.get(taxClass);



            double configured_price  = Double.parseDouble((String)inventory.get("price")) ;
            double tax = isTaxIncludedInPrice() ? configured_price / (1 + taxRate/100.0) * taxRate : configured_price * taxRate;
            double current_price = isTaxIncludedInPrice() ? configured_price - tax : configured_price;
            double regular_price = Double.parseDouble((String)inventory.get("regular_price"));


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
            t.setValue(t.getValue() + tax * quantity.getCount());


            items.add(outItem);
        }


        orderPrice.setListedValue(product_total.get(Price.class,"price").getListedValue() );
        orderPrice.setOfferedValue(product_total.get(Price.class,"price").getOfferedValue() );
        orderPrice.setValue(product_total.get(Price.class,"price").getValue()  + product_tax_total.get(Price.class,"price").getValue() );
        orderPrice.setCurrency("INR");
        quote.setTtl(15L*60L); //15 minutes.

        return items;
    }

    public void loadItems(Order order, JSONArray line_items) {
        Items items = new Items();
        order.setItems(items);
        for (int i = 0 ;i < line_items.size() ; i ++ ){
            JSONObject lineItem = (JSONObject) line_items.get(i);
            createItemFromWooLineItem(items,lineItem);
        }

    }


    private void createItemFromWooLineItem(Items items, JSONObject wooLineItem) {
        Item item = new Item();
        item.setDescriptor(new Descriptor());
        item.setId(BecknIdHelper.getBecknId(String.valueOf(wooLineItem.get("product_id")), adaptor.getSubscriber().getSubscriberId(), Entity.item));
        item.getDescriptor().setName((String)wooLineItem.get("name"));
        item.getDescriptor().setCode((String)wooLineItem.get("sku"));
        if (ObjectUtil.isVoid(item.getDescriptor().getCode())){
            item.getDescriptor().setCode(item.getDescriptor().getName());
        }
        item.getDescriptor().setLongDesc(item.getDescriptor().getName());
        item.getDescriptor().setShortDesc(item.getDescriptor().getName());
        item.setQuantity(new Quantity());
        item.getQuantity().setCount(doubleTypeConverter.valueOf(wooLineItem.get("quantity")).intValue());

        Price price = new Price();
        item.setPrice(price);

        price.setListedValue(doubleTypeConverter.valueOf(wooLineItem.get("subtotal")));
        price.setValue(doubleTypeConverter.valueOf(wooLineItem.get("total")));
        price.setCurrency("INR");
        items.add(item);


    }



    public JSONObject makeWooOrder(Order bo) {

        JSONObject order = new JSONObject();
        JSONObject billing = new JSONObject();
        JSONObject shipping = new JSONObject();

        if (!ObjectUtil.isVoid(bo.getId())){
            order.put("id",BecknIdHelper.getLocalUniqueId(bo.getId(),Entity.order));
        }else {
            order.put("set_paid",false);
        }
        setWooBilling(billing,bo.getBilling());
        if (!billing.isEmpty()){
            order.put("billing",billing);
        }
        setWooShipping(shipping,bo.getFulfillment());
        if (!shipping.isEmpty()){
            order.put("shipping",shipping);
        }

        if (bo.getItems() != null ){
            JSONArray line_items = new JSONArray();
            order.put("line_items",line_items);
            bo.getItems().forEach(boItem->{
                JSONObject item = new JSONObject();
                item.put("product_id",BecknIdHelper.getLocalUniqueId(boItem.getId(),Entity.item));
                item.put("quantity",boItem.getQuantity().getCount());
                line_items.add(item);
            });
        }


        return order;
    }

    private void setWooShipping(JSONObject shipping , Fulfillment fulfillment){
        if (fulfillment == null){
            return;
        }
        User user = fulfillment.getCustomer();

        String[] parts = user.getPerson().getName().split(" ");
        shipping.put("first_name",parts[0]);
        shipping.put("last_name", user.getPerson().getName().substring(parts[0].length()));

        Address address = fulfillment.getEnd().getLocation().getAddress();
        Contact contact = fulfillment.getEnd().getContact();

        Country country = Country.findByName(address.getCountry());
        State state = State.findByCountryAndName(country.getId(),address.getState());
        City city = City.findByStateAndName(state.getId(),address.getCity());

        shipping.put("address_1",address.getDoor()+"," +address.getBuilding());
        shipping.put("address_2",address.getStreet()+","+address.getLocality());
        shipping.put("city", city.getName());
        shipping.put("state",state.getCode());
        shipping.put("postcode",address.getAreaCode());
        shipping.put("country",country.getIsoCode2());
        shipping.put("email",contact.getEmail());
        shipping.put("phone",contact.getPhone());

    }
    private void setWooBilling(JSONObject billing, Billing boBilling) {
        if (boBilling == null){
            return;
        }
        String[] parts = boBilling.getName().split(" ");
        billing.put("first_name",parts[0]);
        billing.put("last_name", boBilling.getName().substring(parts[0].length()));
        if (boBilling.getAddress() != null){
            billing.put("address_1",boBilling.getAddress().getDoor()+"," +boBilling.getAddress().getBuilding());
            billing.put("address_2",boBilling.getAddress().getStreet()+","+boBilling.getAddress().getLocality());

            Country country = Country.findByName(boBilling.getAddress().getCountry());
            State state = State.findByCountryAndName(country.getId(),boBilling.getAddress().getState());
            City city = City.findByStateAndName(state.getId(),boBilling.getAddress().getCity());

            billing.put("city", city.getName());
            billing.put("state",city.getState().getCode());
            billing.put("country",city.getState().getCountry().getIsoCode2());
            billing.put("postcode",boBilling.getAddress().getAreaCode());
        }


        billing.put("email",boBilling.getEmail());
        billing.put("phone",boBilling.getPhone());

    }
    public Order getBecknOrder(JSONObject wooOrder) {
        Order order = new Order();
        order.setPayment(new Payment());
        setPayment(order.getPayment(),wooOrder);
        Quote quote = new Quote();
        order.setQuote(quote);
        quote.setTtl(15*60);
        quote.setPrice(new Price());
        quote.getPrice().setValue(order.getPayment().getParams().getAmount());
        quote.getPrice().setCurrency(order.getPayment().getParams().getCurrency());
        quote.setBreakUp(new BreakUp());
        BreakUpElement element = quote.getBreakUp().createElement("item","Total Product",quote.getPrice());
        quote.getBreakUp().add(element);
        //Delivery breakup to be filled.


        setBoBilling(order,wooOrder);
        order.setId(BecknIdHelper.getBecknId(String.valueOf(wooOrder.get("id")),adaptor.getSubscriber().getSubscriberId(),Entity.order));
        order.setState((String) wooOrder.get("status"));
        loadItems(order,(JSONArray)wooOrder.get("line_items"));

        order.setFulfillment(new Fulfillment());
        order.getFulfillment().setEnd(new FulfillmentStop());
        order.getFulfillment().getEnd().setLocation(new Location());
        order.getFulfillment().getEnd().getLocation().setAddress(new Address());
        order.getFulfillment().getEnd().setContact(new Contact());
        order.getFulfillment().setCustomer(new User());
        order.getFulfillment().setState(order.getState());
        order.getFulfillment().setId(BecknIdHelper.getBecknId(String.valueOf(wooOrder.get("id")),adaptor.getSubscriber().getSubscriberId(),Entity.fulfillment));

        Locations locations = getProviderLocations();
        if (locations.size() > 0) {
            order.getFulfillment().setStart(new FulfillmentStop());
            order.getFulfillment().getStart().setLocation(locations.get(0));
        }

        JSONObject shipping = (JSONObject) wooOrder.get("shipping");
        if (ObjectUtil.isVoid(shipping.get("address_1"))){
            shipping = (JSONObject) wooOrder.get("billing");
        }
        String[] address1_parts = ((String)shipping.get("address_1")).split(",");
        String[] address2_parts = ((String)shipping.get("address_2")).split(",");


        User user = order.getFulfillment().getCustomer();
        user.setPerson(new Person());
        Person person = user.getPerson();
        person.setName(shipping.get("first_name") + " " + shipping.get("last_name"));


        Address address = order.getFulfillment().getEnd().getLocation().getAddress();
        address.setDoor(address1_parts[0]);
        if (address1_parts.length > 1) {
            address.setBuilding(address1_parts[1]);
        }
        address.setStreet(address2_parts[0]);
        if (address2_parts.length > 1) {
            address.setLocality(address2_parts[1]);
        }
        Country country = Country.findByISO((String) shipping.get("country"));
        State state = State.findByCountryAndCode(country.getId(),(String)shipping.get("state"));
        City city = City.findByStateAndName(state.getId(),(String) shipping.get("city"));

        address.setCountry(country.getName());
        address.setState(state.getName());
        address.setPinCode((String)shipping.get("postcode"));
        address.setCity(city.getName());

        order.getFulfillment().getEnd().getContact().setPhone((String)shipping.get("phone"));
        order.getFulfillment().getEnd().getContact().setEmail((String)shipping.get("email"));


        order.setProvider(new Provider());
        order.getProvider().setId(BecknIdHelper.getBecknId(adaptor.getSubscriber().getSubscriberId(),
                adaptor.getSubscriber().getSubscriberId(), Entity.provider));
        order.setProviderLocation(locations.get(0));



        return order;
    }
    private void setPayment(Payment payment, JSONObject wooOrder) {

        if (!booleanTypeConverter.valueOf(wooOrder.get("date_paid"))) {
            payment.setStatus("NOT-PAID");
        }else {
            payment.setStatus("PAID");
        }
        payment.setType("POST-FULFILLMENT");
        payment.setParams(new Params());
        payment.getParams().setCurrency(getStore().getCurrency());
        payment.getParams().setAmount(doubleTypeConverter.valueOf(wooOrder.get("total")));
    }

    private void setBoBilling(Order order, JSONObject wooOrder) {
        Billing billing = new Billing();
        order.setBilling(billing);
        JSONObject wooBilling = (JSONObject) wooOrder.get("billing");
        billing.setName(wooBilling.get("first_name") + " " + wooBilling.get("last_name"));
        billing.setPhone((String) wooBilling.get("phone"));
        billing.setEmail((String) wooBilling.get("email"));
        Address address = new Address();
        billing.setAddress(address);
        address.setName(billing.getName());
        address.setStreet((String)wooBilling.get("address_1"));
        address.setLocality((String)wooBilling.get("address_2"));
        address.setPinCode((String)wooBilling.get("postcode"));

        Country country= Country.findByISO((String)wooBilling.get("country"));
        State state = State.findByCountryAndCode(country.getId(),(String) wooBilling.get("state"));
        City city = City.findByStateAndName(state.getId(),(String)wooBilling.get("city"));

        address.setCountry(country.getName());
        address.setState(state.getName());
        address.setCity(city.getName());

    }




    public <T extends JSONAware> T post(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<>());
    }
    public <T extends JSONAware> T put(String relativeUrl, JSONObject parameter ){
        return post(relativeUrl,parameter,new IgnoreCaseMap<String>(){{
            put("X-HTTP-Method-Override","PUT");
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
