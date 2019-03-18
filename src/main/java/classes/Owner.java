package classes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class Owner {

    private static Properties props;

    private static Producer<String,String> producer;

    private static String REORDER = "reorder";

    private static Properties createProperties(){
        // create instance for properties to access producer configs
        props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        return props;


    }

    private static boolean toSend(String product,String product_name){
        props = createProperties();
        producer = new KafkaProducer<>(props);
        try{
            producer.send(new ProducerRecord<>(REORDER, product_name, product));
            return true;
        }catch (Exception e){
            System.out.println(e.getMessage());
            return false;
        }

    }

    private static void makingRandomOrders() throws InterruptedException{
        List<String> random_ordenrs= new ArrayList<>();
        random_ordenrs.add("Macbook");
        random_ordenrs.add("Matebook");
        random_ordenrs.add("iMac");
        random_ordenrs.add("Lenovo");
        random_ordenrs.add("Ipod");
        random_ordenrs.add("Samsung");
        random_ordenrs.add("iPhone");
        random_ordenrs.add("Huawei");
        random_ordenrs.add("Lenovo");
        random_ordenrs.add("Asus");
        random_ordenrs.add("Acer");
        random_ordenrs.add("Matebook");
        random_ordenrs.add("HP");
        int i=0;
        while (i<200){
            Random rand = new Random();
            int number = rand.nextInt(random_ordenrs.size()) +0;
            int quantity = rand.nextInt(200)+1;
            String product_name = random_ordenrs.get(number);
            String making_order =product_name+"|"+quantity;
            boolean sended = toSend(making_order,product_name);

            if(sended){
                System.out.println("\nProduct is ordered");
            }else{
                System.out.println("\nError in make order");
            }
            i++;
            Thread.sleep(1000);
        }


    }

    private static void menu() throws InterruptedException{
        System.out.println("\n----------Owner---------");
        System.out.println("\n1 - Make order to shop");
        System.out.println("\n2 - Make random orders to shop");
        System.out.println("\n3 - Exit");
        System.out.print("\nOption: ");
        Scanner input = new Scanner(System.in);
        try{
            int option = input.nextInt();
            switch(option){
                case 1:
                    makingOrder();
                    break;
                case 2:
                    makingRandomOrders();
                    menu();
                    break;
                case 3:
                    System.exit(1);
            }
        }catch (NumberFormatException nfe){
            System.out.println("\nInsira novamente");
            menu();
        }

    }

    private static void makingOrder() throws InterruptedException{

        Scanner input = new Scanner(System.in);
        System.out.println("\n-------Making order--------");
        System.out.print("\nProduct name: ");
        String product_name= input.nextLine();
        System.out.print("\nQuantity: ");
        String quantity = input.next();


        String making_order =product_name+"|"+quantity;
        boolean sended = toSend(making_order,product_name);
        System.out.println("\nProduct: "+ making_order);
        if(sended){
            System.out.println("\nProduct is ordered");
            menu();
        }else{
            System.out.println("\nError in make order");
            makingOrder();
        }

    }

    public static void main(String []args ) throws InterruptedException{
        menu();
    }

}
