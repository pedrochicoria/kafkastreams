package classes;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;


//mvn exec:java -Dexec.mainClass=classes.Customer (run in terminal
public class Customer extends Thread {

    private static Properties producer_props;
    private static Properties consumer_props;

    private static Producer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;

    private static String PURCHASES = "purchases";

    private static String topic_name;

    private static EntityManagerFactory emf;
    private static EntityManager em;

    private static Properties createProducerProperties() {
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

    private static Properties createConsumerProperties() {
        consumer_props = new Properties();
        consumer_props.put("bootstrap.servers", "localhost:9092");
        consumer_props.put("group.id", ManagementFactory.getRuntimeMXBean().getName());
        consumer_props.put("enable.auto.commit", "true");
        consumer_props.put("auto.commit.interval.ms", "1000");
        consumer_props.put("session.timeout.ms", "30000");
        consumer_props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer_props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return consumer_props;

    }

    private static boolean toSend(String product, String reference) {

        try {
            producer = new KafkaProducer<>(producer_props);
            producer.send(new ProducerRecord<>(PURCHASES, reference, product));
            producer.flush();
            String []arr =product.split("\\|");
            producer.send(new ProducerRecord<>("purchase-stream", arr[1], product));
            producer.flush();
            producer.close();
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }

    }

    private static void menu() throws InterruptedException{

        System.out.println("\n----------Costumer---------");
        System.out.println("\n1 - Make a purchase");
        System.out.println("\n2 - Make random purchases");
        System.out.println("\n3 - Exit");
        System.out.print("\nOption: ");
        Scanner input = new Scanner(System.in);
        try {
            int option = input.nextInt();
            switch (option) {
                case 1:
                    makingPurchase();
                    menu();
                    break;
                case 2:
                    makeRandomPurchases();
                    menu();
                    break;
                case 3:
                    System.exit(1);
            }
        } catch (NumberFormatException nfe) {
            System.out.println("\nInsira novamente");
            menu();
        }


    }

    private static void makeRandomPurchases() throws InterruptedException{
        int i=0;
        while(i<200){
            Random rand = new Random();
            Query query = em.createQuery("select  p from Product p");
            List<Product> results = query.getResultList();
            int number = rand.nextInt(results.size()-1) +0;
            Product product =results.get(number);
            int quantity = rand.nextInt(200)+1;
            double price = Math.random() * 10000 + 50;
            String making_order = topic_name + "|" + product.getProduct() + "|" + quantity + "|" + price;

            toSend(making_order,Integer.toString(product.getReference()));

            boolean replied = true;

            while (replied) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                int delivery = 0, dennied = 0;
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(" Reply: " + record.value());

                    if (record.key().equals("Delivery")) {
                        delivery = 1;
                    } else if (record.key().equals("Dennied")) {
                        dennied = 1;
                    }

                }
                if (delivery == 1 || dennied == 1) {
                    replied = false;
                }

            }
            i++;
            Thread.sleep(1000);
        }

    }

    private static void makingPurchase() {
        Scanner input = new Scanner(System.in);
        Query query = em.createQuery("select  p from Product p");
        List<Product> results = query.getResultList();
        System.out.println("\n-------Making order--------\n");

        System.out.println("\n-------Products--------\n");
        for (int i = 0; i < results.size(); i++) {
            System.out.println((i + 1) + " - " + results.get(i).getProduct());
        }
        System.out.print("\nChoose a product: ");
        int option = input.nextInt();
        String product_name = results.get(option - 1).getProduct();
        System.out.print("\nQuantity: ");
        String quantity = input.next();
        System.out.print("\nPrice: ");
        double price = input.nextDouble();
        String making_order = topic_name + "|" + product_name + "|" + quantity + "|" + price;
        toSend(making_order, Integer.toString(results.get(option - 1).getReference()));

        System.out.println("\nProduct: " + making_order);


        boolean replied = true;

        while (replied) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            int delivery = 0, dennied = 0;
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(" Reply: " + record.value());

                if (record.key().equals("Delivery")) {
                    delivery = 1;
                } else if (record.key().equals("Dennied")) {
                    dennied = 1;
                }

            }
            if (delivery == 1 || dennied == 1) {
                replied = false;
            }

        }
    }

    public static void connectDatabase() {
        emf = Persistence.createEntityManagerFactory("Shop");
        em = emf.createEntityManager();
    }

    private static void initiate() {
        connectDatabase();
        String[] aux = ManagementFactory.getRuntimeMXBean().getName().split("@");
        System.out.println(aux[0]);
        consumer_props = createConsumerProperties();
        producer_props = createProducerProperties();
        consumer = new KafkaConsumer<>(consumer_props);
        topic_name = aux[0];

        consumer.subscribe(Arrays.asList(topic_name));


    }


    public static void main(String[] args) throws InterruptedException {

        initiate();
        menu();
    }

}
