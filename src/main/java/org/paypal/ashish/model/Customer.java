package org.paypal.ashish.model;

public class Customer {

    private String name;
    private String email;
    private String contactNumber;

    public Customer(final String Name, final String Email, final String ContactNumber) {

        this.name = Name;
        this.email = Email;
        this.contactNumber = ContactNumber;
    }

    public String getEmail() {
        return email;
    }

    public String getName() {
        return name;
    }

    public String getContactNumber() {
        return contactNumber;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setContactNumber(String contactNumber) {
        this.contactNumber = contactNumber;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return String.format("[ Name: %s | Email Address: %s | Phone: %s ]", name, email, contactNumber);
    }
}
