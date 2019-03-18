package classes;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.*;

public class Supplier {
    private static String REORDER = "reorder";
    private static String SHIPMENTS = "shipments";

    private static Properties consumer_props;
    private static Properties producer_props;
    private static KafkaConsumer<String, String> consumer;
    private static Producer<String, String> producer;
    private static List<String> orders;

    private static Properties consumerProperties() {
        consumer_props = new Properties();
        consumer_props.put("bootstrap.servers", "localhost:9092");
        consumer_props.put("group.id", "suplliers");
        consumer_props.put("enable.auto.commit", "true");
        consumer_props.put("auto.commit.interval.ms", "1000");
        consumer_props.put("session.timeout.ms", "30000");
        consumer_props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer_props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return consumer_props;
    }

    private static Properties producerProperties() {
        // create instance for properties to access producer configs
        producer_props = new Properties();

        //Assign localhost id
        producer_props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        producer_props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        producer_props.put("retries", 0);

        //Specify buffer size in config
        producer_props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        producer_props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        producer_props.put("buffer.memory", 33554432);

        producer_props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer_props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        return producer_props;
    }


    private static void initiate() {
        orders = new ArrayList<>();
        consumer_props = consumerProperties();
        consumer = new KafkaConsumer<>(consumer_props);
    }

    private static void receivingOrders() {

        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
            orders.add(record.value());
            System.out.println("Product: " + record.value());
        }

        for (String order : orders) {
            System.out.println("Order: " + order);
        }

    }

    private static void shippingProduct() {
        System.out.println("\n--------Shipping product--------\n");
        for (int i = 0; i < orders.size(); i++) {
            System.out.println((i + 1) + " - " + orders.get(i));
        }
        System.out.print("\nChoose a product to ship: ");
        Scanner input = new Scanner(System.in);
        int option = input.nextInt();
        String product = orders.get(option - 1);
        System.out.println("Product choosed: " + product);
        String shipping_product;
        shipping_product = setSale(product);
        boolean sended = toSend(shipping_product);
        if(sended){
            System.out.println("\nProduct is shipped");

        }else{
            System.out.println("\nError in shipping product");
            shippingProduct();
        }
    }

    private static boolean toSend(String product) {
        String[] splits = product.split("\\|");

        try {
            producer_props = producerProperties();
            producer = new KafkaProducer<>(producer_props);

            producer.send(new ProducerRecord<>(SHIPMENTS, splits[0], product));
            producer.flush();
            producer.send(new ProducerRecord<>("shipments-price",splits[0],splits[1]));
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }

    }


    private static String setSale(String product) {
        orders.remove(product);
        String[] splits = product.split("\\|" );
        String product_name = splits[0];
        String quantity = splits[1];
        System.out.print("\nSet price for " + product_name + ": ");
        Scanner input = new Scanner(System.in);
        double price = input.nextDouble();
        String price_aux = Double.toString(price);

        return product_name + " " + price_aux + " " + quantity;
    }


    private static void addRandomOrders() throws InterruptedException{
        int i=0;
        while (i<200){
            receivingOrders();
            Random rand = new Random();
            int number = rand.nextInt(orders.size());
            String product = orders.get(number);
            double price = Math.random() * 10000 + 50;
            String [] aux = product.split("\\|");

            String ship = aux[0]+"|"+price+"|"+aux[1];
            toSend(ship);

            System.out.println("Encomenda enviada");
            i++;
            Thread.sleep(1000);
        }
    }

    private static void menu() throws InterruptedException {

        consumer.subscribe(Arrays.asList(REORDER));
        while (true) {
            receivingOrders();
            System.out.println("\n----------Supplier-----------\n");
            System.out.println("1 - Add order to shop\n");
            System.out.println("2 - Add random orders to shop\n");
            System.out.println("3 - Exit\n");
            System.out.print("Option: ");
            Scanner input = new Scanner(System.in);
            int option = input.nextInt();
            switch (option) {
                case 1:
                    receivingOrders();
                    shippingProduct();
                    menu();
                    break;
                case 2:
                    addRandomOrders();
                    menu();
                    break;
                case 3:
                    System.exit(1);
                    break;
            }

            Thread.sleep(5000);


        }
    }




    public static void main(String[] args) throws InterruptedException {
        initiate();
        //Kafka Consumer subscribes list of topics here.

        //print the topic name
        System.out.println("Subscribed to topic " + REORDER);


        menu();
    }
}
