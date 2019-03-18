package classes;

import javax.persistence.*;
import java.io.Serializable;

@Entity
public class Product implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int reference;
    private String product;
    private int actual_quantity;
    private int initial_quantity;
    private double price;


    public Product() {
    }

    public Product(String product, int actual_quantity, int initial_quantity,double price) {
        super();
        this.product = product;
        this.actual_quantity = actual_quantity;
        this.initial_quantity=initial_quantity;
        this.price = price;
    }

    public int getReference() {
        return reference;
    }

    public void setReference(int reference) {
        this.reference = reference;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }


    public int getActual_quantity() {
        return actual_quantity;
    }

    public void setActual_quantity(int actual_quantity) {
        this.actual_quantity = actual_quantity;
    }

    public int getInitial_quantity() {
        return initial_quantity;
    }

    public void setInitial_quantity(int initial_quantity) {
        this.initial_quantity = initial_quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Product{" +
                ", reference'=" + reference +
                ", product='" + product + '\'' +
                ", actual_quantity=" + actual_quantity +
                ", initial_quantity=" + initial_quantity +
                ", price=" + price +
                '}';
    }
}