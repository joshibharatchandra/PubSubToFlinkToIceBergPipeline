package org.example.flink.poc;

public class StatementAccount {
    private String accountNo;
    private String accountType;
    private String currency;
    private String branch;
    private String facility;
    private String ifscCode;
    private String micrCode;
    private String openingDate;
    private Double currentBalance;
    private Double currentODLimit;
    private Double drawingLimit;
    private String xnsStartDate;
    private String xnsEndDate;

    // Getters and Setters

    public String getAccountNo() {
        return accountNo;
    }



    public void setAccountNo(String accountNo) {
        this.accountNo = accountNo;
    }

    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public String getFacility() {
        return facility;
    }

    public void setFacility(String facility) {
        this.facility = facility;
    }

    public String getIfscCode() {
        return ifscCode;
    }

    public void setIfscCode(String ifscCode) {
        this.ifscCode = ifscCode;
    }

    public String getMicrCode() {
        return micrCode;
    }

    public void setMicrCode(String micrCode) {
        this.micrCode = micrCode;
    }

    public String getOpeningDate() {
        return openingDate;
    }

    public void setOpeningDate(String openingDate) {
        this.openingDate = openingDate;
    }

    public Double getCurrentBalance() {
        return currentBalance;
    }

    public void setCurrentBalance(Double currentBalance) {
        this.currentBalance = currentBalance;
    }

    public Double getCurrentODLimit() {
        return currentODLimit;
    }

    public void setCurrentODLimit(Double currentODLimit) {
        this.currentODLimit = currentODLimit;
    }

    public Double getDrawingLimit() {
        return drawingLimit;
    }

    public void setDrawingLimit(Double drawingLimit) {
        this.drawingLimit = drawingLimit;
    }

    public String getXnsStartDate() {
        return xnsStartDate;
    }

    public void setXnsStartDate(String xnsStartDate) {
        this.xnsStartDate = xnsStartDate;
    }

    public String getXnsEndDate() {
        return xnsEndDate;
    }

    public void setXnsEndDate(String xnsEndDate) {
        this.xnsEndDate = xnsEndDate;
    }

    @Override
    public String toString() {
        return "StatementAccount{" +
                "accountNo='" + accountNo + '\'' +
                ", accountType='" + accountType + '\'' +
                ", currency='" + currency + '\'' +
                ", branch='" + branch + '\'' +
                ", facility='" + facility + '\'' +
                ", ifscCode='" + ifscCode + '\'' +
                ", micrCode='" + micrCode + '\'' +
                ", openingDate='" + openingDate + '\'' +
                ", currentBalance=" + currentBalance +
                ", currentODLimit=" + currentODLimit +
                ", drawingLimit=" + drawingLimit +
                ", xnsStartDate='" + xnsStartDate + '\'' +
                ", xnsEndDate='" + xnsEndDate + '\'' +
                '}';
    }
}
