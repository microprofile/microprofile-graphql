/*
 * Copyright (c) 2020, 2021 Contributors to the Eclipse Foundation
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
package org.eclipse.microprofile.graphql.tck.apps.superhero.api;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.eclipse.microprofile.graphql.DateFormat;
import org.eclipse.microprofile.graphql.DefaultValue;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.GraphQLException;
import org.eclipse.microprofile.graphql.Mutation;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.NumberFormat;
import org.eclipse.microprofile.graphql.Query;
import org.eclipse.microprofile.graphql.Source;
import org.eclipse.microprofile.graphql.Subscription;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.DuplicateSuperHeroException;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.HeroDatabase;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.HeroLocator;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.SidekickDatabase;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.UnknownHeroException;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.UnknownSidekickException;
import org.eclipse.microprofile.graphql.tck.apps.superhero.db.UnknownTeamException;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.Character;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.Item;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.Sidekick;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.SuperHero;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.Team;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.Token;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.UnknownCharacterException;

import jakarta.inject.Inject;
import jakarta.json.bind.annotation.JsonbDateFormat;
import jakarta.json.bind.annotation.JsonbNumberFormat;

@GraphQLApi
public class HeroFinder {
    private static final Logger LOG = Logger.getLogger(HeroFinder.class.getName());

    @Inject
    private HeroDatabase heroDB;

    @Inject
    private SidekickDatabase sidekickDB;

    @Inject
    private HeroLocator heroLocator;

    @Query
    public Character character(@Name("name") String name) throws UnknownCharacterException {
        LOG.log(Level.INFO, "character invoked [{0}]", name);

        try {
            SuperHero superHero = heroDB.getHero(name);
            return superHero;
        } catch (UnknownHeroException e) {
            try {
                Sidekick sidekick = sidekickDB.getSidekick(name);
                return sidekick;
            } catch (UnknownSidekickException ex) {
                throw new UnknownCharacterException(name);
            }
        }
    }

    @Query
    public SuperHero superHero(@Name("name") @Description("Super hero name, not real name") String name)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "superHero invoked [{0}]", name);
        return Optional.ofNullable(heroDB.getHero(name)).orElseThrow(() -> new UnknownHeroException(name));
    }

    @Query
    @Description("Testing the blacklist of Checked Exceptions")
    public SuperHero exportToFile(@Name("name") @Description("Super hero name, not real name") String name)
            throws IOException {
        LOG.log(Level.INFO, "exportToFile invoked [{0}]", name);
        throw new IOException("No you can not do this.");
    }

    @Query
    @Description("Testing the blacklist of transitive Checked Exceptions")
    public SuperHero exportToCSVFile(@Name("name") @Description("Super hero name, not real name") String name)
            throws CsvIOException {
        LOG.log(Level.INFO, "exportToCSVFile invoked [{0}]", name);
        throw new CsvIOException("No you can not do this.");
    }

    @Query
    @Description("Testing the default blacklist for Runtime Exceptions")
    public SuperHero villian(@Name("name") @Description("Super hero name, not real name") String name) {
        LOG.log(Level.INFO, "villian invoked [{0}]", name);
        throw new RuntimeException("SuperHero can not be a villian");
    }

    @Query
    @Description("Testing the whitelist for Runtime Exceptions")
    public SuperHero weakness(@Name("name") @Description("Super hero name, not real name") String name) {
        LOG.log(Level.INFO, "weakness invoked [{0}]", name);
        throw new WeaknessNotFoundException("Superhero has no weakness");
    }

    @Query
    @Description("Testing the whitelist for transitive Runtime Exceptions")
    public SuperHero findFlaw(@Name("name") @Description("Super hero name, not real name") String name) {
        LOG.log(Level.INFO, "findFlaw invoked [{0}]", name);
        throw new FlawNotFoundException("Superhero has no flaw");
    }

    @Query
    @Description("Testing Errors, as in Java Error")
    public SuperHero wreakHavoc(@Name("name") @Description("Super hero name, not real name") String name) {
        LOG.log(Level.INFO, "wreakHavoc invoked [{0}]", name);
        throw new OutOfMemoryError("a SuperHero has used all the memory");
    }

    @Query
    @Description("List all super heroes in the database")
    public Collection<SuperHero> allHeroes() {
        LOG.info("allHeroes invoked");
        return heroDB.getAllHeroes();
    }

    @Query
    public Collection<SuperHero> allHeroesIn(@DefaultValue("New York, NY") @Name("city") String city) {
        LOG.log(Level.INFO, "allHeroesIn invoked [{0}]", city);
        return allHeroesByFilter(hero -> {
            return city.equals(hero.getPrimaryLocation());
        });
    }

    @Query
    public Collection<SuperHero> allHeroesWithPower(@Name("power") String power) {
        LOG.log(Level.INFO, "allHeroesWithPower invoked [{0}]", power);
        return allHeroesByFilter(hero -> {
            return hero.getSuperPowers().contains(power);
        });
    }

    @Query
    public Collection<SuperHero> allHeroesInTeam(@Name("team") String teamName) throws UnknownTeamException {
        LOG.log(Level.INFO, "allHeroesInTeam invoked [{0}]", teamName);
        return heroDB.getTeam(teamName).getMembers();
    }

    @Query
    public Team getTeam(@Name("team") String teamName) throws UnknownTeamException {
        LOG.log(Level.INFO, "getTeam invoked [{0}]", teamName);
        return heroDB.getTeam(teamName);
    }

    @Query
    public Collection<Team> allTeams() {
        LOG.info("allTeams invoked");
        return heroDB.getAllTeams();
    }

    @Mutation
    public SuperHero createNewHero(@Name("hero") SuperHero newHero)
            throws DuplicateSuperHeroException, UnknownHeroException {
        LOG.log(Level.INFO, "createNewHero invoked [{0}]", newHero);
        heroDB.addHero(newHero);
        return heroDB.getHero(newHero.getName());
    }

    @Mutation
    public Collection<SuperHero> createNewHeroes(@Name("heroes") List<SuperHero> newHeroes)
            throws DuplicateSuperHeroException, UnknownHeroException {
        LOG.log(Level.INFO, "createNewHeroes invoked [{0}]", newHeroes);
        heroDB.addHeroes(newHeroes);
        return newHeroes;
    }

    @Mutation
    public SuperHero[] createNewHeroesWithArray(@Name("heroes") SuperHero[] newHeroes)
            throws DuplicateSuperHeroException, UnknownHeroException {
        LOG.log(Level.INFO, "createNewHeroesWithArray invoked [{0}]", newHeroes);
        List<SuperHero> asList = Arrays.asList(newHeroes);
        heroDB.addHeroes(asList);
        return newHeroes;
    }

    @Mutation
    @Description("Adds a hero to the specified team and returns the updated team.")
    public Team addHeroToTeam(@Name("hero") String heroName,
            @Name("team") String teamName)
            throws UnknownTeamException, UnknownHeroException {

        LOG.log(Level.INFO, "addHeroToTeam invoked [{0}],[{1}]", new Object[]{heroName, teamName});
        return heroDB.getTeam(teamName)
                .addMembers(heroDB.getHero(heroName));
    }

    @Mutation
    @Description("Removes a hero to the specified team and returns the updated team.")
    public Team removeHeroFromTeam(@Name("hero") String heroName,
            @Name("team") String teamName)
            throws UnknownTeamException, UnknownHeroException {
        LOG.log(Level.INFO, "removeHeroFromTeam invoked [{0}],[{1}]", new Object[]{heroName, teamName});
        return heroDB.getTeam(teamName)
                .removeMembers(heroDB.getHero(heroName));
    }

    @Mutation
    @Description("Removes a hero... permanently...")
    public Collection<SuperHero> removeHero(@Name("hero") String heroName) throws UnknownHeroException {
        LOG.log(Level.INFO, "removeHero invoked [{0}]", heroName);
        if (heroDB.removeHero(heroName) == null) {
            throw new UnknownHeroException(heroName);
        }
        return allHeroes();
    }

    @Mutation
    @Description("Gives a hero new equipment")
    public SuperHero provisionHero(@Name("hero") String heroName,
            @DefaultValue(Item.CAPE) @Name("item") Item item)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "provisionHero invoked [{0}],[{1}]", new Object[]{heroName, item});
        SuperHero hero = heroDB.getHero(heroName);
        if (hero == null) {
            throw new UnknownHeroException(heroName);
        }
        hero.getEquipment().add(item);
        return hero;
    }

    @Mutation
    @Description("Removes equipment from a hero")
    public SuperHero removeItemFromHero(@Name("hero") String heroName,
            @Name("itemID") long itemID)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "removeItemFromHero invoked [{0}],[{1}]", new Object[]{heroName, itemID});
        SuperHero hero = heroDB.getHero(heroName);
        if (hero == null) {
            throw new UnknownHeroException(heroName);
        }
        hero.getEquipment().removeIf(i -> {
            return i.getId() == itemID;
        });
        return hero;
    }

    @Mutation
    @Description("Update an item's powerLevel")
    public Item updateItemPowerLevel(@Name("itemID") long itemID,
            @DefaultValue("5") @Name("powerLevel") int newLevel) {
        LOG.log(Level.INFO, "updateItemPowerLevel invoked [{0}],[{1}]", new Object[]{itemID, newLevel});
        Item item = null;
        for (SuperHero hero : allHeroes()) {
            for (Item i : hero.getEquipment()) {
                if (i.getId() == itemID) {
                    item = i;
                    item.setPowerLevel(newLevel);
                }
            }
        }
        return item;
    }

    @Mutation
    @Description("Update a hero's bank account")
    public SuperHero updateBankBalance(@Name("name") String name,
            @Name("bankBalance") double bankBalance) throws UnknownHeroException {
        LOG.log(Level.INFO, "updateBankBalance invoked [{0}],[{1}]", new Object[]{name, bankBalance});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setBankBalance(bankBalance);
        }
        return superHero;
    }

    @Mutation
    @Description("Update a hero's bank account in US Dollar")
    public SuperHero updateBankBalanceInUS(@Name("name") String name,
            @JsonbNumberFormat(value = "¤ ###,###.##", locale = "en-US") @Name("bankBalance") Double bankBalance)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "updateBankBalance invoked [{0}],[{1}]", new Object[]{name, bankBalance});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setBankBalance(bankBalance);
        }
        return superHero;
    }

    @Mutation
    @Description("Update a hero's favourite drink size")
    public SuperHero favouriteDrinkSize(@Name("name") String name,
            @Name("size") float size) throws UnknownHeroException {
        LOG.log(Level.INFO, "favouriteDrinkSize invoked [{0}],[{1}]", new Object[]{name, size});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setFavouriteDrinkSize(size);
        }
        return superHero;
    }

    @Mutation
    @Description("Update a hero's favourite drink size in milliliters")
    public SuperHero favouriteDrinkSizeInML(@Name("name") String name,
            @JsonbNumberFormat(value = "000.00 'kl'") // This should be ignored due to NumberFormat below
            @NumberFormat(value = "###.## 'ml'", locale = "en-GB") @Name("size") Float size)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "favouriteDrinkSizeInML invoked [{0}],[{1}]", new Object[]{name, size});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setFavouriteDrinkSize(size);
        }
        return superHero;
    }

    @Mutation
    @Description("Update a hero's net worth")
    public SuperHero updateNetWorth(@Name("name") String name,
            @Name("netWorth") BigDecimal netWorth) throws UnknownHeroException {
        LOG.log(Level.INFO, "updateNetWorth invoked [{0}],[{1}]", new Object[]{name, netWorth});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setNetWorth(netWorth);
        }
        return superHero;
    }

    @Mutation
    @Description("Update a hero's back account in USD")
    public SuperHero updateNetWorthInUSD(@Name("name") String name,
            @JsonbNumberFormat(value = "¤ 000.00", locale = "en-US") @Name("netWorth") BigDecimal netWorth)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "updateBankBalance invoked [{0}],[{1}]", new Object[]{name, netWorth});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setNetWorth(netWorth);
        }
        return superHero;
    }

    @Mutation
    @Description("All the places this hero has been")
    public SuperHero beenThere(@Name("name") String name,
            @Name("places") Set<String> places) throws UnknownHeroException {
        LOG.log(Level.INFO, "beenThere invoked [{0}],[{1}]", new Object[]{name, places});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setBeenThere(places);
        }
        return superHero;
    }

    @Mutation
    @Description("Log the last place the hero was seen")
    public SuperHero logLocation(@Name("name") String name,
            @Name("coordinates") LinkedList<BigDecimal> coordinates) throws UnknownHeroException {
        LOG.log(Level.INFO, "logLocation invoked [{0}],[{1}]", new Object[]{name, coordinates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setLastKnownCoordinates(coordinates);
        }
        return superHero;
    }

    @Mutation
    @Description("Log the last place the hero was seen (Long Lat)")
    public SuperHero logLocationLongLat(@Name("name") String name,
            @JsonbNumberFormat(value = "00.0000000 longlat", locale = "en-GB") @Name("coordinates") List<BigDecimal> coordinates)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "logLocationLongLat invoked [{0}],[{1}]", new Object[]{name, coordinates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setLastKnownCoordinates(coordinates);
        }
        return superHero;
    }

    @Mutation
    @Description("Log the last few places the hero was seen")
    public SuperHero trackHero(@Name("name") String name,
            @Name("coordinates") List<List<BigDecimal>> coordinates) throws UnknownHeroException {
        LOG.log(Level.INFO, "trackHero invoked [{0}],[{1}]", new Object[]{name, coordinates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setTrack(coordinates);
        }
        return superHero;
    }

    @Mutation
    @Description("Log the last few places the hero was seen (Long Lat)")
    public SuperHero trackHeroLongLat(@Name("name") String name,
            @JsonbNumberFormat(value = "00.0000000 'latlong'") // This should be ignored due to NumberFormat below
            @Name("coordinates") List<List<@NumberFormat(value = "00.0000000 longlat", locale = "en-GB") BigDecimal>> coordinates)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "trackHeroLongLat invoked [{0}],[{1}]", new Object[]{name, coordinates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setTrack(coordinates);
        }
        return superHero;
    }

    @Mutation
    @Description("Log the last place the hero was seen (using an array)")
    public SuperHero logLocationWithArray(@Name("name") String name,
            @Name("coordinates") BigDecimal[] coordinates) throws UnknownHeroException {
        LOG.log(Level.INFO, "logLocationWithArray invoked [{0}],[{1}]", new Object[]{name, coordinates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setLastKnownCoordinates(Arrays.asList(coordinates));
        }
        return superHero;
    }

    @Mutation
    @Description("Log the last place the hero was seen (Long Lat) using an array")
    public SuperHero logLocationLongLatWithArray(@Name("name") String name,
            @JsonbNumberFormat(value = "00.0000000 longlat", locale = "en-GB") @Name("coordinates") BigDecimal[] coordinates)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "logLocationLongLatWithArray invoked [{0}],[{1}]", new Object[]{name, coordinates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setLastKnownCoordinates(Arrays.asList(coordinates));
        }
        return superHero;
    }

    @Mutation
    @Description("Update an item's powerLevel in percentage")
    public Item updateItemPowerLevelPercentage(@Name("itemID") long itemID,
            @JsonbNumberFormat("##'%'") @Name("powerLevel") int newLevel) {
        LOG.log(Level.INFO, "updateItemPowerLevelPercentage invoked [{0}],[{1}]", new Object[]{itemID, newLevel});
        Item item = null;
        for (SuperHero hero : allHeroes()) {
            for (Item i : hero.getEquipment()) {
                if (i.getId() == itemID) {
                    item = i;
                    item.setPowerLevel(newLevel / 20);
                }
            }
        }
        return item;
    }

    @Mutation
    @Description("Check in a superhero")
    public SuperHero checkIn(@Name("name") String name,
            @Name("date") LocalDate localDate) throws UnknownHeroException {
        LOG.log(Level.INFO, "checkIn invoked [{0}],[{1}]", new Object[]{name, localDate});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setDateOfLastCheckin(localDate);
        }
        return superHero;
    }

    @Mutation
    @Description("Check in a superhero")
    public SuperHero checkInWithCorrectDateFormat(@Name("name") String name,
            @JsonbDateFormat("yy dd MM") // This should be ignored due to DateFormat below
            @DateFormat("MM/dd/yyyy") @Name("date") LocalDate localDate) throws UnknownHeroException {
        LOG.log(Level.INFO, "checkInWithCorrectDateFormat invoked [{0}],[{1}]", new Object[]{name, localDate});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setDateOfLastCheckin(localDate);
        }
        return superHero;
    }

    @Mutation
    @Description("Set all the important dates for a certain hero")
    public SuperHero importantDates(@Name("name") String name,
            @Name("dates") List<LocalDate> localDates) throws UnknownHeroException {
        LOG.log(Level.INFO, "importantDates invoked [{0}],[{1}]", new Object[]{name, localDates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setImportantDates(localDates);
        }
        return superHero;
    }

    @Mutation
    @Description("Set all the important dates (US format) for a certain hero")
    public SuperHero importantDatesUS(@Name("name") String name,
            @JsonbDateFormat("yy dd MM") // This should be ignored due to DateFormat below
            @Name("dates") List<@DateFormat("MM/dd/yyyy") LocalDate> localDates) throws UnknownHeroException {
        LOG.log(Level.INFO, "importantDatesUS invoked [{0}],[{1}]", new Object[]{name, localDates});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setImportantDates(localDates);
        }
        return superHero;
    }

    @Mutation
    @Description("Set the time a hero started patrolling")
    public SuperHero startPatrolling(@Name("name") String name,
            @Name("time") LocalTime localTime) throws UnknownHeroException {
        LOG.log(Level.INFO, "startPatrolling invoked [{0}],[{1}]", new Object[]{name, localTime});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setPatrolStartTime(localTime);
        }
        return superHero;
    }

    @Mutation
    @Description("Set the time a hero started patrolling (using formatted time)")
    public SuperHero startPatrollingWithCorrectDateFormat(@Name("name") String name,
            @JsonbDateFormat("HH:mm") @Name("time") LocalTime localTime) throws UnknownHeroException {
        LOG.log(Level.INFO, "startPatrollingWithCorrectDateFormat invoked [{0}],[{1}]", new Object[]{name, localTime});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setPatrolStartTime(localTime);
        }
        return superHero;
    }

    @Mutation
    @Description("Start a battle")
    public SuperHero battle(@Name("name") String name,
            @Name("dateTime") LocalDateTime localDateTime) throws UnknownHeroException {
        LOG.log(Level.INFO, "battle invoked [{0}],[{1}]", new Object[]{name, localDateTime});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setTimeOfLastBattle(localDateTime);
        }
        return superHero;
    }

    @Mutation
    @Description("Start a battle")
    public SuperHero battleWithCorrectDateFormat(@Name("name") String name,
            @JsonbDateFormat("HH:mm:ss dd-MM-yyyy") @Name("dateTime") LocalDateTime localDateTime)
            throws UnknownHeroException {
        LOG.log(Level.INFO, "battleWithCorrectDateFormat invoked [{0}],[{1}]", new Object[]{name, localDateTime});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setTimeOfLastBattle(localDateTime);
        }
        return superHero;
    }

    @Mutation
    @Description("Set the ID Number for a hero")
    public SuperHero idNumber(@Name("name") String name,
            @Name("id") Long idNumber) throws UnknownHeroException {
        LOG.log(Level.INFO, "idNumber invoked [{0}],[{1}]", new Object[]{name, idNumber});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setIdNumber(idNumber);
        }
        return superHero;
    }

    @Mutation
    @Description("Set the ID Number for a hero")
    public SuperHero idNumberWithCorrectFormat(@Name("name") String name,
            @JsonbNumberFormat("ID-00000000") @Name("id") Long idNumber) throws UnknownHeroException {
        LOG.log(Level.INFO, "idNumberWithCorrectFormat invoked [{0}],[{1}]", new Object[]{name, idNumber});
        SuperHero superHero = heroDB.getHero(name);
        if (superHero != null) {
            superHero.setIdNumber(idNumber);
        }
        return superHero;
    }

    @Query
    public String getCurrentLocation(@Name("superHero") @Source SuperHero hero) throws GraphQLException {
        LOG.log(Level.INFO, "currentLocation invoked [{0}]", hero);
        final String heroName = hero.getName();
        return heroLocator.getHeroLocation(heroName)
                .orElseThrow(() -> {
                    return new GraphQLException("Cannot find location for " + heroName,
                            GraphQLException.ExceptionType.DataFetchingException);
                });
    }

    @Name("secretToken")
    public Token generateSecretToken(@Source SuperHero hero,
            @DefaultValue("true") @Name("maskFirstPart") boolean maskFirstPart) throws GraphQLException {
        LOG.log(Level.INFO, "generateSecretToken invoked [{0}],[{1}]", new Object[]{hero, maskFirstPart});

        String uuid = UUID.randomUUID().toString();
        if (maskFirstPart) {
            return new Token(uuid.substring(0, uuid.length() - 4).replaceAll("[A-Za-z0-9]", "*")
                    + uuid.substring(uuid.length() - 4, uuid.length()));
        } else {
            return new Token(uuid);
        }
    }

    public Token generateSuperSecretToken(@Source("superSecretToken") SuperHero hero,
            @DefaultValue("true") @Name("maskFirstPart") boolean maskFirstPart) throws GraphQLException {
        LOG.log(Level.INFO, "generateSuperSecretToken invoked [{0}],[{1}]", new Object[]{hero, maskFirstPart});

        String uuid = UUID.randomUUID().toString();
        if (maskFirstPart) {
            return new Token(uuid.substring(0, uuid.length() - 4).replaceAll("[A-Za-z0-9]", "*")
                    + uuid.substring(uuid.length() - 4, uuid.length()));
        } else {
            return new Token(uuid);
        }
    }

    @Mutation("setRivalTeam")
    public Team setRivalTeam(@Name("teamName") String teamName, @Name("rivalTeam") Team rivalTeam)
            throws UnknownTeamException {

        LOG.log(Level.INFO, "setRivalTeam invoked [{0}],[{1}]", new Object[]{teamName, rivalTeam});
        Team team = heroDB.getTeam(teamName);
        team.setRivalTeam(rivalTeam);
        return team;
    }

    @Query
    public Collection<SuperHero> allHeroesWithError() throws GraphQLException {
        LOG.info("allHeroesWithError invoked");
        List<SuperHero> partialHeroes = new ArrayList<>();
        for (SuperHero hero : heroDB.getAllHeroes()) {
            if ("Starlord".equals(hero.getName())) {
                partialHeroes.add(null);
            } else {
                partialHeroes.add(hero);
            }
        }
        throw new GraphQLException("Failed to find one or more heroes", partialHeroes);
    }

    @Query
    public Collection<SuperHero> allHeroesWithSpecificError() throws SuperHeroLookupException {
        LOG.info("allHeroesWithSpecificError invoked");
        List<SuperHero> partialHeroes = new ArrayList<>();
        for (SuperHero hero : heroDB.getAllHeroes()) {
            if (!"Spider Man".equals(hero.getName())) {
                partialHeroes.add(hero);
            }
        }
        throw new SuperHeroLookupException("Failed to find one or more heroes", partialHeroes);
    }

    @Query
    public Item getItemById(@Name("id") long id) {
        LOG.log(Level.INFO, "getItemById invoked [{0}]", id);
        for (SuperHero hero : allHeroes()) {
            for (Item item : hero.getEquipment()) {
                if (id == item.getId()) {
                    return item;
                }
            }
        }
        return null;
    }

    @Mutation
    public Team createNewTeam(@Name("newTeam") Team newTeam) {
        LOG.log(Level.INFO, "createNewTeam invoked [{0}]", newTeam);
        List<SuperHero> members = newTeam.getMembers();
        Team team = heroDB.createNewTeam(newTeam.getName());
        if (members != null && members.size() > 0) {
            team.setMembers(members);
        }
        team.setRivalTeam(newTeam.getRivalTeam());
        return team;
    }

    @Mutation
    public Team removeTeam(@Name("teamName") String teamName) throws UnknownTeamException {
        LOG.log(Level.INFO, "removeTeam invoked [{0}]", teamName);
        return heroDB.removeTeam(teamName);
    }

    // Subscriptions

    @Subscription
    @Description("Receive updates when any hero changes")
    public Flow.Publisher<SuperHero> heroUpdates() {
        LOG.info("heroUpdates subscription created");
        java.util.concurrent.SubmissionPublisher<SuperHero> publisher =
                new java.util.concurrent.SubmissionPublisher<>();

        // Use a scheduler to emit items periodically, simulating real-time updates.
        // This pattern is realistic for subscriptions that emit data based on events,
        // timers, or periodic polling rather than emitting a fixed collection.
        return new Flow.Publisher<SuperHero>() {
            private final AtomicBoolean started = new AtomicBoolean(false);
            private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            @Override
            public void subscribe(Flow.Subscriber<? super SuperHero> subscriber) {
                publisher.subscribe(subscriber);

                // Start emitting when first subscriber connects
                if (started.compareAndSet(false, true)) {
                    SuperHero[] heroes = heroDB.getAllHeroes().toArray(new SuperHero[0]);
                    AtomicInteger index = new AtomicInteger(0);

                    // Schedule periodic emission of heroes
                    scheduler.scheduleAtFixedRate(() -> {
                        int i = index.getAndIncrement();
                        if (i < 3 && i < heroes.length) {
                            publisher.submit(heroes[i]);
                        } else {
                            publisher.close();
                            scheduler.shutdown();
                        }
                    }, 0, 50, TimeUnit.MILLISECONDS);
                }
            }
        };
    }

    @Subscription
    public Flow.Publisher<SuperHero> heroByName(@Name("name") String heroName) {
        LOG.log(Level.INFO, "heroByName subscription created [{0}]", heroName);
        java.util.concurrent.SubmissionPublisher<SuperHero> publisher =
                new java.util.concurrent.SubmissionPublisher<>();

        // Scheduled emission pattern (see heroUpdates for explanation)
        return new Flow.Publisher<SuperHero>() {
            private final AtomicBoolean started = new AtomicBoolean(false);
            private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            @Override
            public void subscribe(Flow.Subscriber<? super SuperHero> subscriber) {
                publisher.subscribe(subscriber);

                if (started.compareAndSet(false, true)) {
                    scheduler.schedule(() -> {
                        try {
                            SuperHero hero = heroDB.getHero(heroName);
                            if (hero != null) {
                                publisher.submit(hero);
                            }
                            publisher.close();
                            scheduler.shutdown();
                        } catch (UnknownHeroException e) {
                            publisher.closeExceptionally(e);
                            scheduler.shutdown();
                        } catch (Exception e) {
                            publisher.closeExceptionally(e);
                            scheduler.shutdown();
                        }
                    }, 0, TimeUnit.MILLISECONDS);
                }
            }
        };
    }

    @Subscription
    @Description("Receive team updates")
    public Flow.Publisher<Team> teamUpdates() {
        LOG.info("teamUpdates subscription created");
        java.util.concurrent.SubmissionPublisher<Team> publisher = new java.util.concurrent.SubmissionPublisher<>();

        // Scheduled emission pattern (see heroUpdates for explanation)
        return new Flow.Publisher<Team>() {
            private final AtomicBoolean started = new AtomicBoolean(false);
            private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

            @Override
            public void subscribe(Flow.Subscriber<? super Team> subscriber) {
                publisher.subscribe(subscriber);

                if (started.compareAndSet(false, true)) {
                    Team[] teams = heroDB.getAllTeams().toArray(new Team[0]);
                    AtomicInteger index = new AtomicInteger(0);

                    scheduler.scheduleAtFixedRate(() -> {
                        int i = index.getAndIncrement();
                        if (i < 2 && i < teams.length) {
                            publisher.submit(teams[i]);
                        } else {
                            publisher.close();
                            scheduler.shutdown();
                        }
                    }, 0, 50, TimeUnit.MILLISECONDS);
                }
            }
        };
    }

    private Collection<SuperHero> allHeroesByFilter(Predicate<SuperHero> predicate) {
        return heroDB.getAllHeroes()
                .stream()
                .filter(predicate)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
