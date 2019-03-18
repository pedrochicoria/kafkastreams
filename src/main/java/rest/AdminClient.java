package rest;

import java.util.List;
import java.util.Scanner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;


public class AdminClient {

    private static Client client;
    private static WebTarget webTarget;

    private static void menu() {
        System.out.println("---------Administrator----------\n");
        System.out.println("1 - Number of items ever sold\n2 - Number of units sold of each item\n3 - Maximum price of each item sold so far\n4 - Average number of purchases per order of supplies\n5 - Revenue, expenses, and profit of the shop so far\n6 - Item providing the highest profit over the last x minutes\n7 - Query over a range of products for average sales price\n8 - Exit\n");
        System.out.print("Option: ");
        Scanner input = new Scanner(System.in);
        int option = input.nextInt();
        switch (option) {
            case 1:
                getNumberOfItemsSold();
                menu();
                break;
            case 2:
                System.out.println("1 - Ever since units\n2 - Lets x minutes\n");
                System.out.print("Option; ");
                int option1 = input.nextInt();
                switch (option1){
                    case 1:
                        numberOfItemsOfEachItem();
                        menu();
                        break;
                    case 2:
                        numberOfItemsOfEachItemMinutes();
                        menu();
                        break;
                }
                break;
            case 3:
                System.out.println("1 - Ever since maximum prices\n2 - Lets x minutes\n");
                System.out.print("Option; ");
                int option2 = input.nextInt();
                switch (option2){
                    case 1:
                        maximumPriceOfEachItem();
                        menu();
                        break;
                    case 2:
                        maximumPriceOfEachItemMinutes();
                        menu();
                        break;
                }
                break;
            case 4:
                averageNumberOfPurchasesPerOrder();
                menu();
                break;
            case 5:
                revenueExpensesAndProfitOfTheShop();
                break;
            case 6:
                itemProvidingHighestProfit();
                menu();
                break;
            case 7:
                rangeOfProductsForAverageSalesPrice();
                menu();
                break;
            case 8:
                webTarget = client.target("http://localhost:9998/admin/close");
                webTarget.request().post(Entity.entity("close", MediaType.TEXT_PLAIN));
                System.exit(1);
                break;
            default:
                menu();
                break;
        }


    }

    private static void getNumberOfItemsSold() {
        webTarget = client.target("http://localhost:9998/admin/numberofitemssold");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        String response = invocationBuilder.get(String.class);
        System.out.println("Number of items ever sold: "+response);

    }

    private static void numberOfItemsOfEachItem() {
        webTarget = client.target("http://localhost:9998/admin/unitofeachitem");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }

    private static void numberOfItemsOfEachItemMinutes() {
        webTarget = client.target("http://localhost:9998/admin/unitofeachitemMinutes");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }

    private static void maximumPriceOfEachItem() {
        webTarget = client.target("http://localhost:9998/admin/maximumprice");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }

    private static void maximumPriceOfEachItemMinutes() {
        webTarget = client.target("http://localhost:9998/admin/maximumpriceMinutes");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }

    private static void averageNumberOfPurchasesPerOrder() {
        webTarget = client.target("http://localhost:9998/admin/average");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }

    private static void revenueExpensesAndProfitOfTheShop() {
    }

    private static void itemProvidingHighestProfit() {
        webTarget = client.target("http://localhost:9998/admin/bestprofit");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }


    private static void rangeOfProductsForAverageSalesPrice() {
        webTarget = client.target("http://localhost:9998/admin/range");
        Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

        List<String> response = invocationBuilder.get(List.class);

        response.forEach(System.out::println);
    }




    public static void main(String[] args) {
        client = ClientBuilder.newClient();
        webTarget = client.target("http://localhost:9998/admin/");
        webTarget.request().post(Entity.entity("start", MediaType.TEXT_PLAIN));
        //webTarget.request().post(Entity.entity("Joana", MediaType.TEXT_PLAIN));
        menu();

        //Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
        // @SuppressWarnings("unchecked")
        //List<String> response = invocationBuilder.get(List.class);
        //response.forEach(System.out::println);

        //webTarget = client.target("http://localhost:9998/admin/xpto");
        //webTarget.request().post(Entity.entity("Joana", MediaType.TEXT_PLAIN));
        //Invocation.Builder invocationBuilder1 = webTarget.request(MediaType.APPLICATION_JSON);

        //String responde1 = invocationBuilder1.get(String.class);

        //System.out.println("responde 1_ "+responde1);

       webTarget = client.target("http://localhost:9998/admin/close");
        webTarget.request().post(Entity.entity("close", MediaType.TEXT_PLAIN));
    }
}
