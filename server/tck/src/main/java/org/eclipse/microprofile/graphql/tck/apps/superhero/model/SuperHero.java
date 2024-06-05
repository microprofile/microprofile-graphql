/*
 * Copyright (c) 2020 Contributors to the Eclipse Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eclipse.microprofile.graphql.tck.apps.superhero.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.json.bind.annotation.JsonbDateFormat;
import javax.json.bind.annotation.JsonbNumberFormat;
import javax.json.bind.annotation.JsonbProperty;
import javax.json.bind.annotation.JsonbTransient;
import org.eclipse.microprofile.graphql.DateFormat;

import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.NonNull;
import org.eclipse.microprofile.graphql.Query;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.NumberFormat;


public class SuperHero implements Character {
    private List<Team> teamAffiliations;
    private List<@NonNull String> superPowers;
    private PowerSource powerSource;
    private String primaryLocation;
    @Description("Super hero name/nickname")
    private String name;
    private String realName;
    private List<Item> equipment = new ArrayList<>();

    @JsonbDateFormat("MM/dd/yyyy")
    private LocalDate dateOfLastCheckin;

    @JsonbDateFormat("HH:mm")
    private LocalTime patrolStartTime = LocalTime.NOON;

    @JsonbDateFormat("HH:mm:ss dd-MM-yyyy")
    private LocalDateTime timeOfLastBattle;

    @JsonbProperty("colorOfCostume")
    private String costumeColor;

    private List<String> namesOfKnownEnemies; //TODO: consider adding a Villain or Enemy class

    @JsonbNumberFormat("ID-########")
    private Long idNumber;

    @JsonbNumberFormat(value = "¤ ###,###.##",locale = "en-US")
    private Double bankBalance;

    @JsonbNumberFormat(value = "¤ 000.00",locale = "en-US")
    private BigDecimal netWorth;

    @JsonbNumberFormat(value = "###.## 'ml'", locale = "en-GB")
    private Float favouriteDrinkSize;

    private List<BigDecimal> lastKnownCoordinates;

    private List<List<BigDecimal>> track;

    private Set<String> beenThere;

    private ShirtSize sizeOfTShirt;

    private List<LocalDate> importantDates;

    private List<Integer> kidsAges;

    @DateFormat("dd MMMM yyyy")
    private LocalDate birthday;

    public SuperHero(){
    }

    public SuperHero(List<Team> teamAffiliations,
                     List<String> superPowers,
                     PowerSource powerSource,
                     String primaryLocation,
                     String name,
                     String realName) {

        this.teamAffiliations = teamAffiliations;
        this.superPowers = superPowers;
        this.powerSource = powerSource;
        this.primaryLocation = primaryLocation;
        this.name = name;
        this.realName = realName;
    }

    public List<Team> getTeamAffiliations() {
        return teamAffiliations;
    }

    public List<String> getSuperPowers() {
        return superPowers;
    }

    public PowerSource getPowerSource() {
        return powerSource;
    }

    @Description("Location where you are most likely to find this hero")
    public String getPrimaryLocation() {
        return primaryLocation;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getRealName() {
        return realName;
    }

    public void setTeamAffiliations(List<Team> teamAffiliations) {
        this.teamAffiliations = teamAffiliations;
    }

    @Description("Powers that make this hero super")
    public void setSuperPowers(List<String> superPowers) {
        this.superPowers = superPowers;
    }

    public void setPrimaryLocation(String primaryLocation) {
        this.primaryLocation = primaryLocation;
    }

    @NonNull
    @Override
    public void setName(String name) {
        this.name = name;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public List<Item> getEquipment() {
        return equipment;
    }

    public void setEquipment(List<@NonNull Item> equipment) {
        this.equipment = equipment;
    }

    public String getCostumeColor() {
        return costumeColor;
    }

    public void setCostumeColor(String costumeColor) {
        this.costumeColor = costumeColor;
    }

    @Query("knownEnemies")
    public List<String> getNamesOfKnownEnemies() {
        return namesOfKnownEnemies;
    }

    @JsonbTransient
    public void setNamesOfKnownEnemies(List<String> namesOfKnownEnemies) {
        this.namesOfKnownEnemies = namesOfKnownEnemies;
    }

    public LocalDate getDateOfLastCheckin() {
        return dateOfLastCheckin;
    }

    public void setDateOfLastCheckin(LocalDate dateOfLastCheckin) {
        this.dateOfLastCheckin = dateOfLastCheckin;
    }

    public Long getIdNumber() {
        return idNumber;
    }

    public void setIdNumber(Long idNumber) {
        this.idNumber = idNumber;
    }

    public LocalTime getPatrolStartTime() {
        return patrolStartTime;
    }

    public void setPatrolStartTime(LocalTime patrolStartTime) {
        this.patrolStartTime = patrolStartTime;
    }

    public LocalDateTime getTimeOfLastBattle() {
        return timeOfLastBattle;
    }

    public void setTimeOfLastBattle(LocalDateTime timeOfLastBattle) {
        this.timeOfLastBattle = timeOfLastBattle;
    }

    public ShirtSize getSizeOfTShirt() {
        return sizeOfTShirt;
    }

    @Name("tshirtSize")
    public void setSizeOfTShirt(ShirtSize sizeOfTShirt) {
        this.sizeOfTShirt = sizeOfTShirt;
    }

    public Double getBankBalance() {
        return bankBalance;
    }

    public void setBankBalance(Double bankBalance) {
        this.bankBalance = bankBalance;
    }

    public BigDecimal getNetWorth() {
        return netWorth;
    }

    public void setNetWorth(BigDecimal netWorth) {
        this.netWorth = netWorth;
    }

    public Float getFavouriteDrinkSize() {
        return favouriteDrinkSize;
    }

    public void setFavouriteDrinkSize(Float favouriteDrinkSize) {
        this.favouriteDrinkSize = favouriteDrinkSize;
    }

    public List<BigDecimal> getLastKnownCoordinates() {
        return lastKnownCoordinates;
    }

    public void setLastKnownCoordinates(List<BigDecimal> lastKnownCoordinates) {
        this.lastKnownCoordinates = lastKnownCoordinates;
    }

    public List<List<BigDecimal>> getTrack() {
        return track;
    }

    public void setTrack(List<List<BigDecimal>> track) {
        this.track = track;
    }

    public Set<String> getBeenThere() {
        return beenThere;
    }

    public void setBeenThere(Set<String> beenThere) {
        this.beenThere = beenThere;
    }

    public List<@DateFormat("dd/MM") LocalDate> getImportantDates() {
        return importantDates;
    }

    @Name("calendarEntries")
    public void setImportantDates(List<@DateFormat("dd/MM/yy") LocalDate> importantDates) {
        this.importantDates = importantDates;
    }

    @Name("agesOfKids")
    public List<@NumberFormat("0 'years'")Integer> getKidsAges() {
        return kidsAges;
    }

    public void setKidsAges(List<@NumberFormat("0 'years old'")Integer> kidsAges) {
        this.kidsAges = kidsAges;
    }

    public LocalDate getBirthday() {
        return birthday;
    }

    @Name("dateOfBirth")
    public void setBirthday(LocalDate birthday) {
        this.birthday = birthday;
    }

    public enum ShirtSize {
        S,
        M,
        L,
        XL,
        XXL,
        HULK
    }

    @Override
    public String toString() {
        return "SuperHero{"
                + ", superPowers=" + superPowers
                + ", powerSource=" + powerSource
                + ", primaryLocation=" + primaryLocation
                + ", name=" + name
                + ", realName=" + realName
                + ", dateOfLastCheckin=" + dateOfLastCheckin
                + ", patrolStartTime=" + patrolStartTime
                + ", timeOfLastBattle=" + timeOfLastBattle
                + ", costumeColor=" + costumeColor
                + ", namesOfKnownEnemies=" + namesOfKnownEnemies
                + ", idNumber=" + idNumber
                + ", sizeOfTShirt=" + sizeOfTShirt + '}';
    }
}
