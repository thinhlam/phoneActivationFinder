package com.thinhl.model;

import java.io.Serializable;
import java.util.Date;

public class PhoneActivationRecord implements Serializable {
    private String phoneNumber;
    private Date activationDate;
    private Date deActivationDate;


    public PhoneActivationRecord(String phoneNumber, Date activationDate, Date deActivationDate) {
        this.phoneNumber = phoneNumber;
        this.activationDate = activationDate;
        this.deActivationDate = deActivationDate;
    }

    public PhoneActivationRecord(){ }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public void setActivationDate(Date activationDate) {
        this.activationDate = activationDate;
    }

    public void setDeActivationDate(Date deActivationDate) {
        this.deActivationDate = deActivationDate;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public Date getActivationDate() {
        return activationDate;
    }

    public Date getDeActivationDate() {
        return deActivationDate;
    }
}
