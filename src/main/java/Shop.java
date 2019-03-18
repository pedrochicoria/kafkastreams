import classes.Product;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Shop {

    private static final String PURCHASES = "purchases";
    private static final String SHIPMENTS = "shipments";
    private static final String REORDER = "reorder";
    private static EntityManagerFactory emf;
    private static EntityManager em;
    private static Properties consumer_props;
    private static Properties producer_props;
    private static KafkaConsumer<String, String> consumer;
    private static Producer<String, String> producer;


    private static Properties consumerProperties() {

        consumer_props = new Properties();

        consumer_props.put("bootstrap.servers", "localhost:9092");

        consumer_props.put("group.id", "shops");

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
        connectDatabase();
    }

    private static void connectDatabase() {
        emf = Persistence.createEntityManagerFactory("Shop");
        em = emf.createEntityManager();


    }

    private static void startShop() throws InterruptedException {
        consumer.subscribe(Arrays.asList(SHIPMENTS, PURCHASES));

        while (true) {
            reorderFromShop();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Topic: " + record.topic() + " Product: " + record.value());
                // print the offset,key and value for the consumer records.
                if (record.topic().equals(SHIPMENTS)) {
                    boolean add = addDataBase(record.value());
                    if (add) {
                        System.out.println("Product add database");
                    } else {
                        System.out.println("Product not added database");
                    }


                } else if (record.topic().equals(PURCHASES)) {

                    purchasesWork(Integer.parseInt(record.key()), record.value());


                }

            }
            Thread.sleep(1000);
        }
    }

    private static void purchasesWork(int reference, String product) {
        String[] aux = product.split("\\|");
        String topic = aux[0];
        System.out.println(topic);
        String product_name = aux[1];
        System.out.println(product_name);
        String price_aux = aux[3];
        System.out.println(price_aux);
        String quantity = aux[2];
        System.out.println(quantity);

        System.out.println("Reference of product: " + reference);
        System.out.println("Product to buy: " + product);
        Product product1 = em.find(Product.class, reference);
        System.out.println(product1.getProduct());
        System.out.println(Integer.parseInt(quantity));
        if (product1.getActual_quantity() >= Integer.parseInt(quantity)) {
            System.out.println("Existe stock suficiente");
            if (product1.getPrice() <= Double.parseDouble(price_aux)) {
                System.out.println("Satisfaz o preço");
                producer = new KafkaProducer<>(producer_props);
                String reply = "Your purchase is accepted";
                producer.send(new ProducerRecord<>(topic, "Accepted", reply));
                System.out.println("Envia reply: " + reply);
                producer.flush();
                String delivery_product = product_name + "|" + quantity;
                producer.send(new ProducerRecord<>(topic, "Delivery", delivery_product));
                System.out.println("Envia delivery product: " + delivery_product);
                producer.flush();
                String toreply = price_aux;
                for(int i=0;i<Integer.parseInt(quantity);i++){
                    producer.send(new ProducerRecord<>("replies", product_name, toreply));
                }
                System.out.println("Envia delivery product: " + delivery_product);
                producer.flush();
                producer.close();
                em.getTransaction().begin();
                product1.setActual_quantity(product1.getActual_quantity() - Integer.parseInt(quantity));
                em.getTransaction().commit();

            } else {
                System.out.println("Não satisfaz o preço");
                producer = new KafkaProducer<>(producer_props);
                String reply = "Your purchase is dennied";
                producer.send(new ProducerRecord<>(topic, "Dennied", reply));
                System.out.println("Envia reply: " + reply);
                producer.flush();
                producer.close();
            }
        } else {
            System.out.println("Não existe stock suficiente");
            producer = new KafkaProducer<>(producer_props);
            String order = product1.getProduct() + "|" + quantity;
            producer.send(new ProducerRecord<>(REORDER, product1.getProduct(), order));
            System.out.println("Send to supplier: " + order);
            producer.flush();
            producer.close();
            boolean run = true;

            while (run) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Topic: " + record.topic() + " Product: " + record.value());
                    // print the offset,key and value for the consumer records.
                    if (record.topic().equals(SHIPMENTS)) {
                        boolean add = addDataBase(record.value());
                        if (add) {
                            System.out.println("Product add database");
                            run = false;
                        } else {
                            System.out.println("Product not added database");
                        }

                    }

                }
            }

            System.out.println("Existe stock suficiente");
            if (product1.getPrice() <= Double.parseDouble(price_aux)) {
                System.out.println("Satisfaz o preço");
                producer = new KafkaProducer<>(producer_props);
                String reply = "Your purchase is accepted";
                producer.send(new ProducerRecord<>(topic, "Accepted", reply));
                System.out.println("Envia reply: " + reply);
                producer.flush();
                String delivery_product = product_name + "|" + quantity;
                producer.send(new ProducerRecord<>(topic, "Delivery", delivery_product));
                System.out.println("Envia delivery product: " + delivery_product);
                producer.flush();
                String toreply = price_aux;
                for(int i=0;i<Integer.parseInt(quantity);i++){
                    producer.send(new ProducerRecord<>("replies", product_name, toreply));
                }
                producer.flush();
                producer.close();
                em.getTransaction().begin();
                product1.setActual_quantity(product1.getActual_quantity() - Integer.parseInt(quantity));
                em.getTransaction().commit();
            } else {
                System.out.println("Não satisfaz o preço");
                producer = new KafkaProducer<>(producer_props);
                String reply = "Your purchase is dennied";
                producer.send(new ProducerRecord<>(topic, Integer.toString(product1.getReference()), reply));
                System.out.println("Envia reply: " + reply);
                producer.flush();
                producer.close();
            }
            System.out.println("Encomenda enviada");

        }

    }


    private static void reorderFromShop() {
        Query query = em.createQuery("select  p from Product p ");

        List<Product> results = query.getResultList();

        if (!results.isEmpty()) {
            for (Product p : results) {
                if (p.getActual_quantity() < 0.25 * p.getInitial_quantity()) {
                    producer = new KafkaProducer<>(producer_props);

                    System.out.println("Está abaixo dos 25% do valor inicial\n");
                    String product =  p.getProduct() + "|" + ((p.getInitial_quantity() - p.getActual_quantity()));
                    producer.send(new ProducerRecord<>(REORDER,p.getProduct(), product));
                    producer.flush();
                    producer.close();
                    System.out.println("Sent to Reorder");

                }

            }
        }
    }


    private static boolean addDataBase(String product) {
        String[] aux = product.split("\\|");
        String product_name = aux[0];
        String product_price = aux[1];
        String quantity_aux = aux[2];
        em.getTransaction().begin();
        double new_price = Double.parseDouble(product_price);
        int quantity = Integer.parseInt(quantity_aux);
        Query query = em.createQuery("select  p from Product p ");
        List<Product> results = query.getResultList();

        if (!results.isEmpty()) {
            for (Product p : results) {
                if (p.getProduct().equals(product_name)) {
                    Product product1 = em.find(Product.class, p.getReference());
                    product1.setPrice(new_price * 1.30);
                    product1.setActual_quantity(quantity + p.getActual_quantity());
                    em.getTransaction().commit();
                    return true;
                }
            }
            em.merge(new Product(product_name, quantity, quantity, new_price * 1.30));
            em.getTransaction().commit();
            return true;
        } else {
            em.merge(new Product( product_name, quantity, quantity, new_price * 1.30));
            em.getTransaction().commit();
            return true;
        }


    }


    public static void main(String[] args) throws InterruptedException {

        consumer_props = consumerProperties();
        producer_props = producerProperties();
        consumer = new KafkaConsumer<>(consumer_props);
        initiate();

        //Kafka Consumer subscribes list of topics here.
        startShop();

    }
}



