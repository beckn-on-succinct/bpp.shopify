package in.succinct.bpp.shopify.helpers.model;

import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknObjects;
import in.succinct.beckn.BecknObjectsWithId;
import in.succinct.beckn.Tag;
import in.succinct.beckn.Tags;
import org.json.simple.JSONObject;

import java.util.Date;

public class DraftOrder extends ShopifyObjectWithId {
    public DraftOrder(){
        super();
    }
    public DraftOrder(JSONObject eCommerceOrder) {
        super(eCommerceOrder);
    }

    public String getOrderId(){
        return get("order_id");
    }
    
    public String getName(){
        return get("name");
    }
    public void setName(String name){
        set("name",name);
    }

    public Address getShippingAddress(){
        return get(Address.class, "shipping_address");
    }
    public void setShippingAddress(Address shipping_address){
        set("shipping_address",shipping_address);
    }

    public Address getBillingAddress(){
        return get(Address.class, "billing_address");
    }
    public void setBillingAddress(Address billing_address){
        set("billing_address",billing_address);
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

    public String getEmail(){
        return get("email");
    }
    public void setEmail(String email){
        set("email",email);
    }
    public String getStatus(){
        return get("status");
    }
    public void setStatus(String status){
        set("status",status);
    }
    public String getCurrency(){
        return get("currency");
    }
    public void setCurrency(String currency){
        set("currency",currency);
    }

    public String getSource(){
        return get("source");
    }
    public void setSource(String source){
        set("source",source);
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

    public PaymentTerms getPaymentTerms(){
        return get(PaymentTerms.class, "payment_terms");
    }
    public void setPaymentTerms(PaymentTerms payment_terms){
        set("payment_terms",payment_terms);
    }

    public LineItems getLineItems(){
        return get(LineItems.class, "line_items");
    }
    public void setLineItems(LineItems line_items){
        set("line_items",line_items);
    }

    public boolean getTaxesIncluded(){
        return getBoolean("taxes_included");
    }
    public void setTaxesIncluded(boolean taxes_included){
        set("taxes_included",taxes_included);
    }

    public double getTotalTax(){
        return getDouble("total_tax");
    }
    public void setTotalTax(double total_tax){
        set("total_tax",total_tax);
    }
    public double getSubtotalPrice(){
        return getDouble("subtotal_price");
    }
    public void setSubtotalPrice(double subtotal_price){
        set("subtotal_price",subtotal_price);
    }

    public double getTotalPrice(){
        return getDouble("total_price");
    }
    public void setTotalPrice(double total_price){
        set("total_price",total_price);
    }

    public Date getCompletedAt(){
        return getTimestamp("completed_at");
    }
    public void setCompletedAt(Date completed_at){
        set("completed_at",completed_at,TIMESTAMP_FORMAT);
    }

    public ShippingLine getShippingLine(){
        return get(ShippingLine.class, "shipping_line");
    }
    public void setShippingLine(ShippingLine shipping_line){
        set("shipping_line",shipping_line);
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
    }
    public static class  NoteAttributes extends BecknObjects<Tag> {

    }
    public static class LineItem extends ShopifyObjectWithId{
        public String getVariantId(){
            return get("variant_id");
        }
        public void setVariantId(String variant_id){
            set("variant_id",variant_id);
        }
        public String getProductId(){
            return get("product_id");
        }
        public void setProductId(String product_id){
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

        public double getQuantity(){
            return getDouble("quantity");
        }
        public void setQuantity(double quantity){
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

    }

    public static class TaxLine extends ShopifyObjectWithId{
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
        public double getTitle(){
            return getDouble("title");
        }
        public void setTitle(double title){
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
    public static class TaxLines extends BecknObjectsWithId<TaxLine>{

    }
    public static class LineItems extends BecknObjectsWithId<LineItem>{

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

    }
}
