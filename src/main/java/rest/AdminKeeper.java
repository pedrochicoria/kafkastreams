package rest;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/admin")
public class AdminKeeper {
    //XXX: we have several threads...
    private static String PURCHASES = "purchase-stream";
    private static String REORDER = "reorder";
    private static String SHIPMENTS = "shipments";
    private static String REPLIES = "replies";
    private static String SHIPMENTSPRICE = "shipments-price";
    private static String PURCHASETABLE = "purchasesTable";
    private static String UNITSTABLE = "unitsTable";
    private static String UNITSMINUTESTABLE = "uniMinutesTable";
    private static String PRICESTABLE = "pricesTable";
    private static String PRICESMINUTESTABLE = "priMinutesTable";
    private static String REORDERTABLE = "reordersTable";
    private static String SHIPMENTSTABLE = "shipmentsTable";
    private static String SHIPMENTSPRICETABLE = "shipmentspriceTable";
    private static String AVERAGETABLE ="averageTable";
    private static String BESTPROFITTABLE ="profitTable";

    private static KafkaStreams streams;

    private static String minutes;

/*
    @POST
    public void addStudent(String student) {
        System.out.println("Called post addStudent with parameter: " + student);
        System.out.println("Thread = " + Thread.currentThread().getName());
        students.add(student);
    }*/

    @POST
    public void startStreams(String start) throws InterruptedException {

        streams = createStream();
        streams.setStateListener(new MyListener());
        streams.start();
        Thread.sleep(1000);
        System.out.println("Streams have started");
    }

    @Path("minutes")
    @POST
    public void postMinutes(String minutos) {
        minutes = minutos;
    }


    private KafkaStreams createStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,2);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> replies = builder.stream(REPLIES);

        KStream<String, String> purchases = builder.stream(PURCHASES);

        KStream<String,String> shipments = builder.stream(SHIPMENTS);

        KStream<String,String> shipmentsPrice = builder.stream(SHIPMENTSPRICE);
        KTable<String,Long> count_shipments = shipments.
                groupByKey().
                count(Materialized.as(SHIPMENTSTABLE));
        count_shipments.mapValues(v -> " " + v).toStream();

        KTable<String, Long> count_purchases = purchases.
                groupByKey()
                .count(Materialized.as(PURCHASETABLE));
        count_purchases.mapValues(v -> " " + v).toStream();

        KTable<String,String> averagePurchasesShipments= count_purchases
                .join(count_shipments, ( sum, count) -> Double.toString(sum.doubleValue()/count.doubleValue()),
                        Materialized.as(AVERAGETABLE));
        averagePurchasesShipments.mapValues(v -> " " + v).toStream();

        KTable<String, Long> countreplies = replies.
                groupByKey().count(Materialized.as(UNITSTABLE));
        countreplies.mapValues(v -> " " + v).toStream();

        KTable<String,String> minPrice = shipmentsPrice.
                groupByKey().
                reduce((oldval, newval) ->Double.toString(Math.min(Double.parseDouble(oldval),Double.parseDouble(newval))),
                        Materialized.as(SHIPMENTSPRICETABLE));
        minPrice.mapValues(v -> "" + v).toStream();



        KTable<Windowed<String>, Long> lastminutesreplies = replies.groupByKey().
                windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5))).
                count(Materialized.as(UNITSMINUTESTABLE));
        lastminutesreplies.toStream((wk, v) -> wk.key()).map((k, v) -> new KeyValue<>(k, "" + k + "-->" + v)).to("test");

        KTable<String, String> maximumprice = replies.
                groupByKey().reduce((oldval, newval) -> Double.toString(Math.max(Double.parseDouble(oldval), Double.parseDouble(newval))), Materialized.as(PRICESTABLE));
        maximumprice.mapValues(v -> "" + v).toStream();

        KTable<Windowed<String>, String> lastminutesprices = replies.groupByKey().
                windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5))).
                reduce((oldval, newval) -> Double.toString(Math.max(Double.parseDouble(oldval), Double.parseDouble(newval))), Materialized.as(PRICESMINUTESTABLE));
        lastminutesprices.toStream((wk, v) -> wk.key()).map((k, v) -> new KeyValue<>(k, "" + k + "-->" + v)).to("test");

        KTable<String,String> bestProfit = maximumprice.
                join(minPrice,( sum,count )-> Double.toString(Double.parseDouble(sum) - Double.parseDouble(count)),
                Materialized.as(BESTPROFITTABLE));
        bestProfit.mapValues(v -> " " + v).toStream();

        return new KafkaStreams(builder.build(), props);

    }


    public void test(String table){
        ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(table, QueryableStoreTypes.keyValueStore());

        System.out.println();

        // Get the values for a range of keys available in this application instance
        KeyValueIterator<String, Long> all = keyValueStore.all();
        while (all.hasNext()) {
            KeyValue<String, Long> next = all.next();
            System.out.println("Item: " + next.key + " Quantity >> " + next.value);
        }
    }


    @Path("numberofitemssold")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getNumberItemsSold() {

        //streams.setStateListener(new MyListener());
        System.out.println(streams.state());

        ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(UNITSTABLE, QueryableStoreTypes.keyValueStore());

        System.out.println();

        // Get the values for a range of keys available in this application instance
        KeyValueIterator<String, Long> all = keyValueStore.all();
        while (all.hasNext()) {
            KeyValue<String, Long> next = all.next();
            System.out.println("Item: " + next.key + " Quantity >> " + next.value);
        }
        all.close();
        return Long.toString(keyValueStore.approximateNumEntries());
    }


    @Path("unitofeachitem")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> unitsOfEachItem() {
        //streams.setStateListener(new MyListener());
        System.out.println(streams.state());

        ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(UNITSTABLE, QueryableStoreTypes.keyValueStore());
        System.out.println();
        KeyValueIterator<String, Long> all = keyValueStore.all();
        List<String> aux = new ArrayList<>();
        while (all.hasNext()) {
            KeyValue<String, Long> next = all.next();
            System.out.println("Item: " + next.key + " Quantity >> " + next.value);
            String product = "Item: " + next.key + " Units: " + next.value;
            aux.add(product);
        }
        return aux;

    }

    @Path("unitofeachitemMinutes")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> unitsOfEachItemMinutes() throws InterruptedException {

        System.out.println(streams.state());
        ReadOnlyWindowStore<String, Long> keyValueStore = streams.store(UNITSMINUTESTABLE, QueryableStoreTypes.windowStore());


        KeyValueIterator<Windowed<String>, Long> all = keyValueStore.all();
        // KeyValueIterator<String, Long> all = keyValueStore.all();

        List<String> aux = new ArrayList<>();
        while (all.hasNext()) {
            KeyValue<Windowed<String>, Long> next = all.next();
            System.out.println("Item: " + next.key.key() + " Quantity >> " + next.value);
            String product = "Item: " + next.key.key() + " Units: " + next.value;
            aux.add(product);
        }
        return aux;


    }

    @Path("maximumprice")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> maximumPriceOfEachItem() {
        System.out.println(streams.state());

        ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store(PRICESTABLE, QueryableStoreTypes.keyValueStore());
        System.out.println();
        KeyValueIterator<String, String> all = keyValueStore.all();
        List<String> aux = new ArrayList<>();
        while (all.hasNext()) {
            KeyValue<String, String> next = all.next();
            System.out.println("Item: " + next.key + " Price >> " + next.value);

            String product = "Item: " + next.key + " Max price: " + next.value;
            aux.add(product);
        }
        return aux;
    }

    @Path("maximumpriceMinutes")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> maximumPriceOfEachItemMinutes() throws InterruptedException {

        ReadOnlyWindowStore<String, String> keyValueStore = streams.store(PRICESMINUTESTABLE, QueryableStoreTypes.windowStore());
        KeyValueIterator<Windowed<String>, String> all = keyValueStore.all();

        List<String> aux = new ArrayList<>();
        while (all.hasNext()) {
            KeyValue<Windowed<String>, String> next = all.next();
            System.out.println("Item: " + next.key.key() + " Price >> " + next.value);

            String product = "Item: " + next.key.key() + " Max price: " + next.value;
            aux.add(product);
        }
        return aux;

    }

    @Path("average")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> averagePurchases(){
        try {
            ReadOnlyKeyValueStore<String, Double> keyValueStore = streams.store(AVERAGETABLE, QueryableStoreTypes.keyValueStore());
            KeyValueIterator<String, Double> range = keyValueStore.all();
            List<String> aux = new ArrayList<>();

            while (range.hasNext()) {
                KeyValue<String, Double> next = range.next();
                System.out.println("Item: " + next.key + " Average: " + next.value);
                String product = "Item: " + next.key + " Average: " + next.value;
                aux.add(product);

            }
            test(PURCHASETABLE);
            test(SHIPMENTSTABLE);
            return aux;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Path("bestprofit")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getBestProfit(){
        try {
            test(PRICESTABLE);
            test(SHIPMENTSPRICETABLE);
            ReadOnlyKeyValueStore<String,String> keyValueStore = streams.store(BESTPROFITTABLE, QueryableStoreTypes.keyValueStore());
            KeyValueIterator<String, String> range = keyValueStore.all();
            List<String> aux = new ArrayList<>();
            while (range.hasNext()) {
                KeyValue<String, String> next = range.next();
                System.out.println("Item: " + next.key + " Profit: " + next.value);
                String product = "Item: " + next.key + " Profit: " + next.value;
                aux.add(product);

            }

            return aux;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Path("range")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> rangeOfProducts() {
        ReadOnlyKeyValueStore<String, String> keyValueStore = streams.store(PRICESTABLE, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, String> range = keyValueStore.range("Macbook", "Lenovo");
        List<String> aux = new ArrayList<>();

        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            System.out.println("Item: " + next.key + " Price: " + next.value);
            String product = "Item: " + next.key + "Price; " + next.value;
            aux.add(product);
        }

        return aux;
    }



    @Path("close")
    @POST
    public void closeStreams(String close) {
        streams.close();
        System.out.println("Streams have closed");
    }


}

class MyListener implements KafkaStreams.StateListener {


    @Override
    public void onChange(KafkaStreams.State state, KafkaStreams.State state1) {
        System.out.println("Vim do " + state + " Vou para o " + state1);
    }
}
