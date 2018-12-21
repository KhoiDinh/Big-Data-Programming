package edu.sjsu.cs185C;

import java.io.Serializable;
import java.util.Comparator;

public class SalaryRecord implements Serializable {
  /**
	* Record fields: Employee Name,Job Title,Base Pay,Overtime Pay,Other Pay,Benefits,Total Pay,Total Pay & Benefits,Year,Notes,Agency,Status
	*/
  private static final long serialVersionUID = 1L;

  private String employeeName;
  private String jobTitle;
  private float basePay;
  private float overtimePay;
  private float otherPay;
  private float benefits;
  private float totalPay;
  private float totalPayAndBenefits;
  private int year;
  private String notes;
  private String agency;
  private String status;

  public SalaryRecord() {
    this.employeeName = null;
    this.jobTitle = null;
    this.basePay = 0;
    this.overtimePay = 0;
    this.otherPay = 0;
    this.benefits = 0;
    this.totalPay = 0;
    this.totalPayAndBenefits = 0;
    this.year = 0;
    this.notes = null;
    this.agency = null;
    this.status = null;
}

  public SalaryRecord(String employeeName,
                      String jobTitle,
                      float basePay,
                      float overtimePay,
                      float otherPay,
                      float benefits,
                      float totalPay,
                      float totalPayAndBenefits,
                      int year,
                      String notes,
                      String agency,
                      String status) {
    this.employeeName = employeeName;
    this.jobTitle = jobTitle;
    this.basePay = basePay;
    this.overtimePay = overtimePay;
    this.otherPay = otherPay;
    this.benefits = benefits;
    this.totalPay = totalPay;
    this.totalPayAndBenefits = totalPayAndBenefits;
    this.year = year;
    this.notes = notes;
    this.agency = agency;
    this.status = status;
  }

  public String getEmployeeName() { return employeeName; }
  public String getJobTitle() { return jobTitle; }
  public float getBasePay() { return basePay; }
  public float getOvertimePay() { return overtimePay; }
  public float getOtherPay() { return otherPay; }
  public float getBenefits() { return benefits; }
  public float getTotalPay() { return totalPay; }
  public float getTotalPayAndBenefits() { return totalPayAndBenefits; }
  public int getYear() { return year; }
  public String getNotes() { return notes; }
  public String getAgency() { return agency; }
  public String getStatus() { return status; }


  public void setEmployeeName(String employeeName ) { this.employeeName = employeeName; }
  public void setJobTitle(String jobTitle) { this.jobTitle = jobTitle; }
  public void setBasePay(float basePay) { this.basePay = basePay; }
  public void setOvertimePay(float overtimePay) { this.overtimePay = overtimePay; }
  public void setOtherPay(float otherPay) { this.otherPay = otherPay; }
  public void setBenefits(float benefits) { this.benefits = benefits; }
  public void setTotalPay(float totalPay) { this.totalPay = totalPay; }
  public void setTotalPayAndBenefits(float totalPayAndBenefits) { this.totalPayAndBenefits = totalPayAndBenefits; }
  public void setYear(int year) { this.year = year; }
  public void setNotes(String notes) { this.notes = notes; }
  public void setAgency(String agency) { this.agency = agency; }
  public void setStatus(String status) { this.status = status; }

}

