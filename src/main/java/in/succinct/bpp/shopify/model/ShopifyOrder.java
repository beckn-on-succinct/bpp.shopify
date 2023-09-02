package in.succinct.bpp.shopify.model;

import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.collab.db.model.config.Country;
import com.venky.swf.plugins.collab.db.model.config.State;
import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknObjects;
import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.CancellationReasons.CancellationReasonCode;
import in.succinct.beckn.Fulfillment.FulfillmentStatus;
import in.succinct.beckn.IssueSubCategory;
import in.succinct.beckn.Order.Return;
import in.succinct.beckn.Order.Status;
import in.succinct.beckn.ReturnReasons.ReturnReasonCode;
import in.succinct.beckn.Tag;
import in.succinct.bpp.core.db.model.ProviderConfig;
import in.succinct.bpp.shopify.adaptor.ECommerceSDK;
import in.succinct.bpp.shopify.model.Products.Metafield;
import in.succinct.bpp.shopify.model.Products.Metafields;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ShopifyOrder extends ShopifyObjectWithId {
    public ShopifyOrder(){
        super();
    }
    public ShopifyOrder(JSONObject eCommerceOrder) {
        super(eCommerceOrder);
    }

    public String getAppId(){
        return get("app_id");
    }

    public Address getBillingAddress(){
        return get(Address.class, "billing_address");
    }
    public void setBillingAddress(Address billing_address){
        set("billing_address",billing_address);
    }


    public String getCurrency(){
        return get("currency");
    }
    public void setCurrency(String currency){
        set("currency",currency);
    }

    public Date getCreatedAt(){
        return getTimestamp("created_at");
    }
    public void setCreatedAt(Date created_at){
        set("created_at",created_at,TIMESTAMP_FORMAT);
    }

    public String getCancelReason(){
        return get("cancel_reason");
    }
    public void setCancelReason(String cancel_reason){
        set("cancel_reason",cancel_reason);
    }

    public Date getCancelledAt(){
        return getTimestamp("cancelled_at");
    }
    public void setCancelledAt(Date cancelled_at){
        set("cancelled_at",cancelled_at,TIMESTAMP_FORMAT);
    }

    public Date getCompletedAt(){
        return getTimestamp("completed_at");
    }
    public void setCompletedAt(Date completed_at){
        set("completed_at",completed_at,TIMESTAMP_FORMAT);
    }



    public String getEmail(){
        return get("email");
    }
    public void setEmail(String email){
        set("email",email);
    }

    public boolean isEstimatedTaxes(){
        return getBoolean("estimated_taxes");
    }
    public void setEstimatedTaxes(boolean estimated_taxes){
        set("estimated_taxes",estimated_taxes);
    }


    public String getFinancialStatus(){
        return get("financial_status");
    }
    public void setFinancialStatus(String financial_status){
        set("financial_status",financial_status);
    }


    public Fulfillments getFulfillments(){
        return get(Fulfillments.class, "fulfillments");
    }
    public void setFulfillments(Fulfillments fulfillments){
        set("fulfillments",fulfillments);
    }

    public LineItems getLineItems(){
        return get(LineItems.class, "line_items");
    }
    public void setLineItems(LineItems line_items){
        set("line_items",line_items);
    }


    public Long getLocationId(){
        return get("location_id");
    }
    public void setLocationId(Long location_id){
        set("location_id",location_id);
    }


    public String getName(){
        return get("name");
    }
    public void setName(String name){
        set("name",name);
    }

    public String getNote(){
        return get("note");
    }
    public void setNote(String note){
        set("note",note);
    }


    public NoteAttributes getNoteAttributes(){
        return get(NoteAttributes.class, "note_attributes");
    }
    public void setNoteAttributes(NoteAttributes note_attributes){
        set("note_attributes",note_attributes);
    }

    public long getNumber(){
        return getLong("number");
    }

    public long getOrderNumber(){
        return getLong("order_number");
    }


    public PaymentTerms getPaymentTerms(){
        return get(PaymentTerms.class, "payment_terms");
    }
    public void setPaymentTerms(PaymentTerms payment_terms){
        set("payment_terms",payment_terms);
    }


    public String getPhone(){
        return get("phone");
    }
    public void setPhone(String phone){
        set("phone",phone);
    }



    public Address getShippingAddress(){
        return get(Address.class, "shipping_address");
    }
    public void setShippingAddress(Address shipping_address){
        set("shipping_address",shipping_address);
    }


    public String getSourceName(){
        return get("source_name");
    }
    public void setSourceName(String source_name){
        set("source_name",source_name);
    }


    public double getSubtotalPrice(){
        return getDouble("subtotal_price");
    }
    public void setSubtotalPrice(double subtotal_price){
        set("subtotal_price",subtotal_price);
    }


    public String getTags(){
        return get("tags");
    }
    public void setTags(String tags){
        set("tags",tags);
    }
    public boolean getTaxesIncluded(){
        return getBoolean("taxes_included");
    }
    public void setTaxesIncluded(boolean taxes_included){
        set("taxes_included",taxes_included);
    }

    public boolean isTest(){
        return getBoolean("test");
    }
    public void setTest(boolean test){
        set("test",test);
    }

    public Transactions getTransactions(){
        return get(Transactions.class, "transactions");
    }
    public void setTransactions(Transactions transactions){
        set("transactions",transactions);
    }


    public double getTotalTax(){
        return getDouble("total_tax");
    }
    public void setTotalTax(double total_tax){
        set("total_tax",total_tax);
    }

    public double getTotalPrice(){
        return getDouble("total_price");
    }
    public void setTotalPrice(double total_price){
        set("total_price",total_price);
    }

    public double getTotalWeight(){
        return getDouble("total_weight");
    }
    public void setTotalWeight(double total_weight){
        set("total_weight",total_weight);
    }


    public Date getUpdatedAt(){
        return getTimestamp("updated_at");
    }
    public void setUpdatedAt(Date updated_at){
        set("updated_at",updated_at,TIMESTAMP_FORMAT);
    }



    public Date getInvoiceSentAt(){
        return getTimestamp("invoice_sent_at");
    }
    public void setInvoiceSentAt(Date invoice_sent_at){
        set("invoice_sent_at",invoice_sent_at,TIMESTAMP_FORMAT);
    }

    public String getInvoiceUrl(){
        return get("invoice_url");
    }
    public void setInvoiceUrl(String invoice_url){
        set("invoice_url",invoice_url);
    }


    public Refunds getRefunds(){
        return get(Refunds.class, "refunds");
    }
    public void setRefunds(Refunds refunds){
        set("refunds",refunds);
    }
    public static class Refunds extends BecknObjectsWithId<Refund> {
        public Refunds() {
        }

        public Refunds(JSONArray array) {
            super(array);
        }

        public Refunds(String payload) {
            super(payload);
        }
    }

    public ShippingLine getShippingLine(){
        ShippingLines shippingLines = getShippingLines();
        if (shippingLines.size() > 0) {
            return shippingLines.get(0);
        }
        return null;
    }
    public void setShippingLine(ShippingLine shipping_line){
        ShippingLines shippingLines = getShippingLines();
        if (shippingLines.size() > 0){
            shippingLines.clear();
        }
        shippingLines.add(shipping_line);
    }

    public ShippingLines getShippingLines(){
        return get(ShippingLines.class, "shipping_lines", true);
    }
    public void setShippingLines(ShippingLines shipping_lines){
        set("shipping_lines",shipping_lines);
    }

    public Metafields getMetafields(){
        return get(Metafields.class, "metafields");
    }
    public void setMetafields(Metafields metafields){
        set("metafields",metafields);
    }

    static Map<String, Status> orderStatusMap = new HashMap<>() {{
        put("restocked",Status.Cancelled);
        put("ready_for_pickup",Status.In_progress);
        put("delivered",Status.Completed);
        put("in_transit",Status.In_progress);
        put("out_for_delivery",Status.In_progress);
        put("failure",Status.In_progress);
        put("confirmed",Status.In_progress);

    }};

    static Map<String, FulfillmentStatus> fulfillmentStatusMap = new HashMap<>() {{
        put("ready_for_pickup",FulfillmentStatus.Packed);
        put("delivered",FulfillmentStatus.Order_delivered);
        put("in_transit",FulfillmentStatus.Order_picked_up);
        put("out_for_delivery",FulfillmentStatus.Out_for_delivery);
        put("failure",FulfillmentStatus.Cancelled);
        put("confirmed",FulfillmentStatus.Packed);
    }};

    public Status getStatus(){
        String s =  get("fulfillment_status");
        Status status = s == null ? null : orderStatusMap.get(s);
        Set<String> statuses = new HashSet<>(){{
            add("pending");
            add("open");
            add("success");
        }};

        if (status == null){
            if (getFulfillments() != null && getFulfillments().size() > 0){
                for (Fulfillment fulfillment : getFulfillments()){
                    String shipmentStatus = fulfillment.getShipmentStatus();
                    String fulfilmentStatus = fulfillment.getStatus();
                    if (!statuses.contains(fulfilmentStatus)){
                        continue;
                    }
                    status = orderStatusMap.get(shipmentStatus);
                    break;
                }
            }
            if (status == null){
                if (getCancelledAt() != null) {
                    status = Status.Cancelled;
                } else if ("fulfilled".equals(s) && isDelivered()) {
                    status = Status.Completed;
                } else if (getFulfillments().size() > 0){
                    status = Status.In_progress;
                } else if (!ObjectUtil.isVoid(getInvoiceUrl())) {
                    status = Status.In_progress;
                }else {
                    status = Status.Accepted;
                }
            }
        }
        return status;
    }

    public FulfillmentStatus getFulfillmentStatus(){
        String s =  get("fulfillment_status");
        FulfillmentStatus status = s == null ? null : fulfillmentStatusMap.get(s);
        if (status == null){
            Set<String> statuses = new HashSet<>(){{
                add("pending");
                add("open");
                add("success");
            }};

            if (getFulfillments() != null && getFulfillments().size() > 0){
                for (Fulfillment fulfillment : getFulfillments()){
                    String shipmentStatus = fulfillment.getShipmentStatus();
                    String fulfilmentStatus = fulfillment.getStatus();
                    if (!statuses.contains(fulfilmentStatus)){
                        continue;
                    }
                    status = fulfillmentStatusMap.get(shipmentStatus);
                    break;
                }
            }
            if (status == null){
                if (getCancelledAt() != null) {
                    status = FulfillmentStatus.Cancelled;
                } else if ("fulfilled".equals(s)) {
                    if (isDelivered()) {
                        status = FulfillmentStatus.Order_delivered;
                    }else {
                        status = FulfillmentStatus.Order_picked_up;
                    }
                }else if (!ObjectUtil.isVoid(getInvoiceUrl())) {
                    status = FulfillmentStatus.Packed;
                }else {
                    status = FulfillmentStatus.Pending;
                }
            }
        }
        return status;
    }

    public boolean isSettled(){
        return getBoolean("settled");
    }
    public void setSettled(boolean settled){
        set("settled",settled);
    }

    public boolean isDelivered(){
        return getBoolean("delivered");
    }
    public void setDelivered(boolean delivered){
        set("delivered",delivered);
    }

    public String getTrackingUrl(){
        return get("tracking_url");
    }
    public void setTrackingUrl(String tracking_url){
        set("tracking_url",tracking_url);
    }

    public void loadMetaFields(ECommerceSDK helper) {
        Metafields metafields = getMetafields();
        if (metafields != null){
            return;
        }

        JSONObject meta = helper.get(String.format("/orders/%s/metafields.json",StringUtil.valueOf(getId())),new JSONObject());
        JSONArray metafieldArray = (JSONArray) meta.get("metafields");
        metafields = new Metafields(metafieldArray);
        setMetafields(metafields);

        for (Metafield m : metafields){
            if (m.getKey().equals("settled")) {
                setSettled(Database.getJdbcTypeHelper("").getTypeRef(Boolean.class).getTypeConverter().valueOf(m.getValue()));
            }else if (m.getKey().equals("invoice_url")){
                JSONObject o = helper.graphql(String.format("{node(id:\"%s\"){ id ... on %s { url  , alt ,fileStatus} }}",m.getValue(),m.getValue().split("/")[3]));
                JSONObject data = (JSONObject) o.get("data");
                JSONObject node = (JSONObject) data.get("node");
                if (node != null) {
                    setInvoiceUrl((String) node.get("url"));
                }
            }else if (m.getKey().equals("delivered")){
                setDelivered(Database.getJdbcTypeHelper("").getTypeRef(Boolean.class).getTypeConverter().valueOf(m.getValue()));
            }else if (m.getKey().equals("tracking_url")){
                setTrackingUrl(m.getValue());
            }else if (m.getKey().equals("settled_amount")) {
                setSettledAmount(Database.getJdbcTypeHelper("").getTypeRef(Double.class).getTypeConverter().valueOf(m.getValue()));
            }
        }

    }
    public double getSettledAmount(){
        return getDouble("settled_amount");
    }
    public void setSettledAmount(double settled_amount){
        set("settled_amount",settled_amount);
    }


    public static class ShippingLines extends BecknObjects<ShippingLine>{
        public ShippingLines(){
            super();
        }
    }
    public static class ShippingLine extends BecknObject{
        public boolean getCustom(){
            return getBoolean("custom");
        }
        public void setCustom(boolean custom){
            set("custom",custom);
        }

        public String getTitle(){
            return get("title");
        }
        public void setTitle(String title){
            set("title",title);
        }
        public double getPrice(){
            return getDouble("price");
        }
        public void setPrice(double price){
            set("price",price);
        }
        
        public String getPhone(){
            return get("phone");
        }
        public void setPhone(String phone){
            set("phone",phone);
        }

        public String getSource(){
            return get("source");
        }
        public void setSource(String source){
            set("source",source);
        }

        public TaxLines getTaxLines(){
            return get(TaxLines.class, "tax_lines");
        }
        public void setTaxLines(TaxLines tax_lines){
            set("tax_lines",tax_lines);
        }

        public String getCode(){
            return get("code");
        }
        public void setCode(String code){
            set("code",code);
        }
    }
    public static class  NoteAttributes extends BecknObjects<Tag> {

    }
    public static class LineItem extends BecknObject{
        public String getId(){
            return StringUtil.valueOf(get("id"));
        }

        public long getVariantId(){
            return get("variant_id");
        }
        public void setVariantId(long variant_id){
            set("variant_id",variant_id);
        }
        public long getProductId(){
            return get("product_id");
        }
        public void setProductId(long product_id){
            set("product_id",product_id);
        }
        public String getName(){
            return get("name");
        }
        public void setName(String name){
            set("name",name);
        }

        public String getVariantTitle(){
            return get("variant_title");
        }
        public void setVariantTitle(String variant_title){
            set("variant_title",variant_title);
        }
        public String getVendor(){
            return get("vendor");
        }
        public void setVendor(String vendor){
            set("vendor",vendor);
        }

        public int getQuantity(){
            return getInteger("quantity");
        }
        public void setQuantity(int quantity){
            set("quantity",quantity);
        }
        public boolean getGiftCard(){
            return getBoolean("gift_card");
        }
        public void setGiftCard(boolean gift_card){
            set("gift_card",gift_card);
        }

        public String getFulfillmentService(){
            return get("fulfillment_service");
        }
        public void setFulfillmentService(String fulfillment_service){
            set("fulfillment_service",fulfillment_service);
        }

        public BecknObject getProperties(){
            return get(BecknObject.class, "properties");
        }
        public void setProperties(BecknObject properties){
            set("properties",properties);
        }

        public double getAppliedDiscount(){
            return getDouble("applied_discount");
        }
        public void setAppliedDiscount(double applied_discount){
            set("applied_discount",applied_discount);
        }

        public TaxLines getTaxLines(){
            return get(TaxLines.class, "tax_lines");
        }
        public void setTaxLines(TaxLines tax_lines){
            set("tax_lines",tax_lines);
        }

        public double getPrice(){
            return getDouble("price");
        }
        public void setPrice(double price){
            set("price",price);
        }

        public double getGrams(){
            return getDouble("grams");
        }
        public void setGrams(double grams){
            set("grams",grams);
        }

        public boolean getRequiresShipping(){
            return getBoolean("requires_shipping");
        }
        public void setRequiresShipping(boolean requires_shipping){
            set("requires_shipping",requires_shipping);
        }

        public String getSku(){
            return get("sku");
        }
        public void setSku(String sku){
            set("sku",sku);
        }

        public boolean getTaxable(){
            return getBoolean("taxable");
        }
        public void setTaxable(boolean taxable){
            set("taxable",taxable);
        }

        public double getFulfillableQuantity(){
            return getDouble("fulfillable_quantity");
        }
        public void setFulfillableQuantity(double fulfillable_quantity){
            set("fulfillable_quantity",fulfillable_quantity);
        }
        public Long getFulfillmentLineItemId(){
            return get("fulfillment_line_item_id");
        }
        public void setFulfillmentLineItemId(Long fulfillment_line_item_id){
            set("fulfillment_line_item_id",fulfillment_line_item_id);
        }

    }

    public static class TaxLine extends BecknObject{
        public double getPrice(){
            return getDouble("price");
        }
        public void setPrice(double price){
            set("price",price);
        }

        public double getRate(){
            return getDouble("rate");
        }
        public void setRate(double rate){
            set("rate",rate);
        }
        public String getTitle(){
            return get("title");
        }
        public void setTitle(String title){
            set("title",title);
        }



    }

    public static class PaymentTerms extends BecknObject {
        public double getAmount(){
            return getDouble("amount");
        }
        public void setAmount(double amount){
            set("amount",amount);
        }

        public String getCurrency(){
            return get("currency");
        }
        public void setCurrency(String currency){
            set("currency",currency);
        }

        public String getPaymentTermsName(){
            return get("payment_terms_name");
        }
        public void setPaymentTermsName(String payment_terms_name){
            set("payment_terms_name",payment_terms_name);
        }
        
        public String getPaymentTermsType(){
            return get("payment_terms_type");
        }
        public void setPaymentTermsType(String payment_terms_type){
            set("payment_terms_type",payment_terms_type);
        }
        public int getDueInDays(){
            return getInteger("due_in_days");
        }
        public void setDueInDays(int due_in_days){
            set("due_in_days",due_in_days);
        }

        public PaymentSchedules getPaymentSchedules(){
            return get(PaymentSchedules.class, "payment_schedules");
        }
        public void setPaymentSchedules(PaymentSchedules payment_schedules){
            set("payment_schedules",payment_schedules);
        }

    }

    public static class PaymentSchedules extends BecknObjects<PaymentSchedule> {


    }
    public static class PaymentSchedule extends BecknObject {
        public double getAmount(){
            return getDouble("amount");
        }
        public void setAmount(double amount){
            set("amount",amount);
        }
        public String getCurrency(){
            return get("currency");
        }
        public void setCurrency(String currency){
            set("currency",currency);
        }

        public Date getIssuedAt(){
            return getTimestamp("issued_at");
        }
        public void setIssuedAt(Date issued_at){
            set("issued_at",issued_at,TIMESTAMP_FORMAT);
        }
        public Date getDueAt(){
            return getTimestamp("due_at");
        }
        public void setDueAt(Date due_at){
            set("due_at",due_at,TIMESTAMP_FORMAT);
        }
        public Date getCompletedAt(){
            return getTimestamp("completed_at");
        }
        public void setCompletedAt(Date completed_at){
            set("completed_at",completed_at,TIMESTAMP_FORMAT);
        }
        public String getExpectedPaymentMethod(){
            return get("expected_payment_method");
        }
        public void setExpectedPaymentMethod(String expected_payment_method){
            set("expected_payment_method",expected_payment_method);
        }


    }
    public static class TaxLines extends BecknObjects<TaxLine>{

    }
    public static class LineItems extends BecknObjects<LineItem>{

    }

    public static class Address extends BecknObject{
        public String getAddress1(){
            return get("address1");
        }
        public void setAddress1(String address1){
            set("address1",address1);
        }

        public String getAddress2(){
            return get("address2");
        }
        public void setAddress2(String address2){
            set("address2",address2);
        }

        public String getCity(){
            return get("city");
        }
        public void setCity(String city){
            set("city",city);
        }

        public String getCountry(){
            return get("country");
        }
        public void setCountry(String country){
            set("country",country);
        }
        public String getCountryCode(){
            return get("country_code");
        }
        public void setCountryCode(String country_code){
            set("country_code",country_code);
        }
        public String getFirstName(){
            return get("first_name");
        }
        public void setFirstName(String first_name){
            set("first_name",first_name);
        }
        public String getLastName(){
            return get("last_name");
        }
        public void setLastName(String last_name){
            set("last_name",last_name);
        }
        public double getLatitude(){
            return getDouble("latitude");
        }
        public void setLatitude(double latitude){
            set("latitude",latitude);
        }
        public double getLongitude(){
            return getDouble("longitude");
        }
        public void setLongitude(double longitude){
            set("longitude",longitude);
        }

        public String getName(){
            return get("name");
        }
        public void setName(String name){
            set("name",name);
        }
        public String getPhone(){
            return get("phone");
        }
        public void setPhone(String phone){
            set("phone",phone);
        }
        public String getProvince(){
            return get("province");
        }
        public void setProvince(String province){
            set("province",province);
        }
        public String getProvinceCode(){
            return get("province_code");
        }
        public void setProvinceCode(String province_code){
            set("province_code",province_code);
        }
        public String getZip(){
            return get("zip");
        }
        public void setZip(String zip){
            set("zip",zip);
        }

        public in.succinct.beckn.Address getAddress (){
            in.succinct.beckn.Address address = new in.succinct.beckn.Address();
            address.setName(getFirstName() + " " + getLastName());
            if (getAddress1() != null){
                String[] parts = getAddress1().split(",");
                if (parts.length > 0) address.setDoor(parts[0]);
                if (parts.length > 1) {
                    StringBuilder remaining = new StringBuilder();
                    for (int i = 1 ; i< parts.length ; i++) {
                        remaining.append(remaining.length() > 0 ? "," : "" );
                        remaining.append(parts[i]);
                    }
                    address.setBuilding(remaining.toString());
                }
            }
            if (getAddress2() != null){
                String[] parts = getAddress2().split(",");
                if (parts.length > 0) address.setStreet(parts[0]);
                if (parts.length > 1) {
                    StringBuilder remaining = new StringBuilder();
                    for (int i = 1 ; i< parts.length ; i++) {
                        remaining.append(remaining.length() > 0 ? "," : "" );
                        remaining.append(parts[i]);
                    }
                    address.setLocality(remaining.toString());
                }
            }

            address.setPinCode(getZip());
            Country country= Country.findByISO(getCountryCode());
            address.setCountry(country.getName());

            State state =  null ;
            if (getProvince() != null) {
                state = State.findByCountryAndName(country.getId(),getProvince());
            }else if (getProvinceCode() != null){
                state = State.findByCountryAndName(country.getId(),getProvinceCode());
            }

            if (state != null) {
                address.setState(state.getName());
                City city = City.findByStateAndName(state.getId(), getCity());
                address.setCity(city.getName());
            }
            return address;
        }
    }

    public static class Fulfillment extends ShopifyObjectWithId{
        public Fulfillment(){
            super();
        }
        public Fulfillment(JSONObject fulfillment){
            super(fulfillment);
        }
        public BecknStrings getTrackingUrls(){
            return get(BecknStrings.class, "tracking_urls");
        }

        public String getStatus(){
            return get("status");
        }
        public void setStatus(String status){
            set("status",status);
        }

        public String getShipmentStatus(){
            return get("shipment_status");
        }
        public void setShipmentStatus(String shipment_status){
            set("shipment_status",shipment_status);
        }

        public LineItems getLineItems(){
            return get(LineItems.class, "line_items");
        }
        public void setLineItems(LineItems line_items){
            set("line_items",line_items);
        }



    }
    public static class Fulfillments extends BecknObjectsWithId<Fulfillment> {
        public Fulfillments(){
            super();
        }
        public Fulfillments(JSONArray array){
            super(array);
        }
    }

    public static class Transactions extends BecknObjects<Transaction> {
        public Transactions(){
            super();
        }
        public Transactions(JSONArray array){
            super(array);
        }
    }

    public static class Transaction extends ShopifyObjectWithId {
        public Transaction(JSONObject o){
            super(o);
        }
        public Transaction(){

        }
        public long getParentId(){
            return getLong("parent_id");
        }
        public void setParentId(long parent_id){
            set("parent_id",parent_id);
        }

        public boolean isTest(){
            return getBoolean("test");
        }
        public void setTest(boolean test){
            set("test",test);
        }
        public String getKind(){
            return get("kind");
        }
        public void setKind(String kind){
            set("kind",kind);
        }
        public String getStatus(){
            return get("status");
        }
        public void setStatus(String status){
            set("status",status);
        }

        public double getAmount(){
            return getDouble("amount");
        }
        public void setAmount(double amount){
            set("amount",amount);
        }

        public String getCurrency(){
            return get("currency");
        }
        public void setCurrency(String currency){
            set("currency",currency);
        }

    }


    public static class Refund extends ShopifyObjectWithId {
        public Refund(){

        }
        public Refund(JSONObject o){
            super(o);
        }

        public Date getCreatedAt(){
            return getDate("created_at",TIMESTAMP_FORMAT);
        }

        public String getNote(){
            return get("note");
        }
        public void setNote(String note){
            set("note",note);
        }

        public Date getProcessedAt(){
            return getTimestamp("processed_at");
        }
        public void setProcessedAt(Date processed_at){
            set("processed_at",processed_at,TIMESTAMP_FORMAT);
        }

        public RefundLineItems getRefundLineItems(){
            return get(RefundLineItems.class, "refund_line_items");
        }
        public void setRefundLineItems(RefundLineItems refund_line_items){
            set("refund_line_items",refund_line_items);
        }

        public Transactions getTransactions(){
            return get(Transactions.class, "transactions");
        }
        public void setTransactions(Transactions transactions){
            set("transactions",transactions);
        }

        public static class RefundLineItems extends BecknObjectsWithId<RefundLineItem>{
            public RefundLineItems() {
            }

            public RefundLineItems(JSONArray array) {
                super(array);
            }

            public RefundLineItems(String payload) {
                super(payload);
            }
        }
        public static class RefundLineItem extends ShopifyObjectWithId {
            public RefundLineItem() {
            }

            public RefundLineItem(String payload) {
                super(payload);
            }

            public RefundLineItem(JSONObject object) {
                super(object);
            }

            public RefundLineItem(LineItem lineItem, int quantity , CancellationReasonCode cancellationReasonCode, ProviderConfig config){
                this(lineItem,quantity,cancellationReasonCode,null,config);
            }
            public RefundLineItem(LineItem lineItem, int quantity , ReturnReasonCode returnReasonCode, ProviderConfig config){
                this(lineItem,quantity,null,returnReasonCode,config);
            }
            private RefundLineItem(LineItem lineItem, int quantity , CancellationReasonCode cancellationReasonCode, ReturnReasonCode returnReasonCode, ProviderConfig config){
                setLineItemId(Long.parseLong(lineItem.getId()));
                setQuantity(quantity);

                double loss = lineItem.getPrice() * quantity;

                if (returnReasonCode != null){
                    if (returnReasonCode.getIssueSubCategory() == IssueSubCategory.ITEM_QUALITY || loss < config.getMaxWriteOffAmountToAvoidRTO()) {
                        setRestockType("no_restock"); // Leave the product no need to pick up !!
                    }else {
                        setRestockType("return");
                    }
                }else {
                    setRestockType("cancel");
                }

            }
            public String getRestockType(){
                return get("restock_type");
            }
            public void setRestockType(String restock_type){
                set("restock_type",restock_type);
            }

            public LineItem getLineItem(){
                return get(LineItem.class, "line_item");
            }
            public void setLineItem(LineItem line_item){
                set("line_item",line_item);
            }
            
            public long getLineItemId(){
                return getLong("line_item_id");
            }
            public void setLineItemId(long line_item_id){
                set("line_item_id",line_item_id);
            }

            public int getQuantity(){
                return getInteger("quantity");
            }
            public void setQuantity(int quantity){
                set("quantity",quantity);
            }


            public String getReturnId(){
                return get("return_id");
            }
            public void setReturnId(String return_id){
                set("return_id",return_id);
            }

        }

    }
}
