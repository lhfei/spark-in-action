/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.lhfei.spark.df.streaming.adult;
/**
 * <block>
	age: continuous.
	workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
	fnlwgt: continuous.
	education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
	education-num: continuous.
	marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
	occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
	relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
	race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
	sex: Female, Male.
	capital-gain: continuous.
	capital-loss: continuous.
	hours-per-week: continuous.
	native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
 	income: >50K, <=50K.
 * </block>
 * 
 * @version 1.0.0
 *
 * @author Hefei Li
 *
 * @since Sep 3, 2016
 */
public class Adult {
	public Adult() {}
	
	
	public Adult(int age, String workclass, int fnlwgt, String education, int education_num, String marital_status,
			String occupation, String relationship, String race, String sex, int capital_gain, int capital_loss,
			int hours_per_week, String native_country, String income) {
		
		this.age			= age            ;
		this.workclass		= workclass      ;
		this.fnlwgt			= fnlwgt         ;
		this.education		= education      ;
		this.education_num	= education_num  ;
		this.marital_status	= marital_status ;
		this.occupation		= occupation     ;
		this.relationship	= relationship   ;
		this.race			= race           ;
		this.sex			= sex            ;
		this.capital_gain	= capital_gain   ;
		this.capital_loss	= capital_loss   ;
		this.hours_per_week	= hours_per_week ;
		this.native_country	= native_country ;
		this.income			= income         ;
	}
	
	
	
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getWorkclass() {
		return workclass;
	}
	public void setWorkclass(String workclass) {
		this.workclass = workclass;
	}
	public int getFnlwgt() {
		return fnlwgt;
	}
	public void setFnlwgt(int fnlwgt) {
		this.fnlwgt = fnlwgt;
	}
	public String getEducation() {
		return education;
	}
	public void setEducation(String education) {
		this.education = education;
	}
	public int getEducation_num() {
		return education_num;
	}
	public void setEducation_num(int education_num) {
		this.education_num = education_num;
	}
	public String getMarital_status() {
		return marital_status;
	}
	public void setMarital_status(String marital_status) {
		this.marital_status = marital_status;
	}
	public String getOccupation() {
		return occupation;
	}
	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}
	public String getRelationship() {
		return relationship;
	}
	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}
	public String getRace() {
		return race;
	}
	public void setRace(String race) {
		this.race = race;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public int getCapital_gain() {
		return capital_gain;
	}
	public void setCapital_gain(int capital_gain) {
		this.capital_gain = capital_gain;
	}
	public int getCapital_loss() {
		return capital_loss;
	}
	public void setCapital_loss(int capital_loss) {
		this.capital_loss = capital_loss;
	}
	public int getHours_per_week() {
		return hours_per_week;
	}
	public void setHours_per_week(int hours_per_week) {
		this.hours_per_week = hours_per_week;
	}
	public String getNative_country() {
		return native_country;
	}
	public void setNative_country(String native_country) {
		this.native_country = native_country;
	}
	public String getIncome() {
		return income;
	}
	public void setIncome(String income) {
		this.income = income;
	}
	
	private int age;
	private String workclass;
	private int fnlwgt;
	private String education;
	private int education_num;
	private String marital_status;
	private String occupation;
	private String relationship;
	private String race;
	private String sex;
	private int capital_gain;
	private int capital_loss;
	private int hours_per_week;
	private String native_country;
	private String income;
}