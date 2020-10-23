package com.example.batch.DAO;

import java.time.LocalDate;

public class UserDAO {
    private String firstname;
    private String dob;

    public UserDAO() {
    }

    public UserDAO(String firstname, String dob) {
        this.firstname = firstname;
        this.dob = dob;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getDob() {
        return dob;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    @Override
    public String toString() {
        return "UserDAO{" +
                "firstname='" + firstname + '\'' +
                ", dob='" + dob + '\'' +
                '}';
    }
}
