package com.mas2022datascience.generalworkersprint.processor;

public enum SprintTypes {
  SPRINT("SPRINT"), SHORTACCELERATION("ACC"), INCREMENTALRUN("CULM"), JOG("CONST");

  final private String abbreviation;

  SprintTypes(String abbreviation) {
    this.abbreviation = abbreviation;
  }

  public String getAbbreviation() {
    return abbreviation;
  }
}
