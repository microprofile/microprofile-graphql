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
package org.eclipse.microprofile.graphql.tck.apps.superhero.db;

import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.eclipse.microprofile.graphql.tck.apps.superhero.model.SuperHero;
import org.eclipse.microprofile.graphql.tck.apps.superhero.model.Team;

@ApplicationScoped
public class HeroDatabase {
    private final Map<String, SuperHero> allHeroes = new HashMap<>();
    private final Map<String, Team> allTeams = new HashMap<>();

    private void init(@Observes @Initialized(ApplicationScoped.class) Object init) {

        try {
            Jsonb jsonb = JsonbBuilder.create();
            String mapJson = getInitalJson();
            addHeroes(jsonb.fromJson(mapJson,
                      new ArrayList<SuperHero>(){}.getClass().getGenericSuperclass()));
            getHero("Iron Man").setNamesOfKnownEnemies(Arrays.asList("Whiplash", "Mandarin"));
            getTeam("Avengers").setDailyStandupMeeting(OffsetTime.parse("11:05:00+02:00"));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public SuperHero getHero(String name) throws UnknownHeroException {
        SuperHero superHero = allHeroes.get(name);

        if (superHero == null) {
            throw new UnknownHeroException(name);
        }

        return superHero;
    }

    public Team getTeam(String name) throws UnknownTeamException {
        Team team = allTeams.get(name);
        if (team == null) {
            throw new UnknownTeamException(name);
        }
        return team;
    }

    public Collection<SuperHero> getAllHeroes() {
        return allHeroes.values();
    }

    public Collection<Team> getAllTeams() {
        return allTeams.values();
    }

    public int addHeroes(Collection<SuperHero> heroes) {
        int count = 0;
        for (SuperHero hero : heroes) {
            try {
                addHero(hero);
                count++;
            } catch (DuplicateSuperHeroException ex) {
                System.out.println("Already added : " + hero.getName());
            }
        }
        return count;
    }

    public void addHero(SuperHero hero) throws DuplicateSuperHeroException {
        allHeroes.put(hero.getName(), hero);
        List<Team> teams = hero.getTeamAffiliations();
        if (teams != null) {
            ListIterator<Team> iter = teams.listIterator();
            while (iter.hasNext()) {
                Team team = iter.next();
                Team existingTeam = allTeams.get(team.getName());
                if (existingTeam == null) {
                    existingTeam = createNewTeam(team.getName());
                }
                iter.set(existingTeam);
                List<SuperHero> members = existingTeam.getMembers();
                if (members == null) {
                    members = new ArrayList<>();
                    existingTeam.setMembers(members);
                }
                members.add(hero);
            }
        }
    }

    public SuperHero removeHero(String heroName) {
        SuperHero hero = allHeroes.remove(heroName);
        if (hero == null) {
            return null;
        }
        for (Team team : getAllTeams()) {
            team.removeMembers(hero);
        }
        return hero;
    }

    public Team createNewTeam(String teamName, SuperHero...initialMembers) {
        Team newTeam = new Team();
        newTeam.setName(teamName);
        newTeam.addMembers(initialMembers);
        allTeams.put(teamName, newTeam);
        return newTeam;
    }

    public Team removeHeroesFromTeam(Team team, SuperHero... heroes) {
        team.removeMembers(heroes);
        for (SuperHero hero : heroes) {
            List<Team> teamAffiliations = hero.getTeamAffiliations();
            if (teamAffiliations != null) {
                teamAffiliations.remove(team);
            }
        }
        return team;
    }

    public Team removeHeroesFromTeam(Team team, Collection<SuperHero> heroes) {
        return removeHeroesFromTeam(team, heroes.toArray(new SuperHero[]{}));
    }

    public Team removeTeam(String teamName) throws UnknownTeamException {
        Team team = allTeams.remove(teamName);
        if (team == null) {
            throw new UnknownTeamException(teamName);
        }
        return removeHeroesFromTeam(team, allHeroes.values());
    }

    private static String getInitalJson() {
        return "[" +
            "{" +
             "\"name\":\"Iron Man\"," +
             "\"realName\":\"Tony Stark\"," +
             "\"primaryLocation\":\"Los Angeles, CA\"," +
             "\"superPowers\":[\"wealth\",\"engineering\"]," +
             "\"powerSource\":{\"netWorth\":12400000000.00," +
             "\"teamAffiliations\":[{\"name\":\"Avengers\"}]," +
             "\"equipment\":[{\"id\": 1001, \"name\": \"Iron Man Suit\", \"powerLevel\": 18, " +
             "\"height\": 1.8, \"weight\": 120.7, \"supernatural\": false, \"dateCreated\": \"12 February 1967 at 11:45 in Africa/Johannesburg\"," +
             "\"dateLastUsed\": \"30 Jan 2020 at 17:55 in zone +0200\" }]," +
             "\"colorOfCostume\":\"Red\"," +
             "\"idNumber\":\"ID-12345678\"," +
             "\"dateOfLastCheckin\":\"09/09/2019\"," +
             "\"timeOfLastBattle\":\"08:30:01 06-09-2019\"," +
             "\"patrolStartTime\":\"08:00\"" +
            "}," +
            "{" +
             "\"name\":\"Spider Man\"," +
             "\"realName\":\"Peter Parker\"," +
             "\"primaryLocation\":\"New York, NY\"," +
             "\"superPowers\":[\"Spidey Sense\",\"Wall-Crawling\",\"Super Strength\",\"Web-shooting\"]," +
             "\"powerSource\":{\"dateOfEvent\":\"06/18/2016\",\"medicalNotes\":\"Bit by a radioactive spider\"}," +
             "\"teamAffiliations\":[{\"name\":\"Avengers\"}]," +
             "\"colorOfCostume\":\"Red\"," +
             "\"idNumber\":\"ID-78904321\"," +
             "\"dateOfLastCheckin\":\"09/01/2019\"," +
             "\"timeOfLastBattle\":\"11:12:45 30-08-2019\"," +
             "\"patrolStartTime\":\"16:00\"" +
            "}," +
            "{" +
             "\"name\":\"Starlord\"," +
             "\"realName\":\"Peter Quill\"," +
             "\"primaryLocation\":\"Outer Space\"," +
             "\"superPowers\":[\"Ingenuity\",\"Humor\",\"Dance moves\"]," +
             "\"teamAffiliations\":[{\"name\":\"Guardians of the Galaxy\"}]," +
             "\"colorOfCostume\":\"Brown\"," +
             "\"idNumber\":\"ID-23409876\"," +
             "\"dateOfLastCheckin\":\"08/27/2019\"," +
             "\"timeOfLastBattle\":\"05:17:33 26-08-2019\"," +
             "\"patrolStartTime\":\"12:30\"" +
            "}," +
            "{" +
             "\"name\":\"Wolverine\"," +
             "\"realName\":\"James Howlett, aka Logan\"," +
             "\"primaryLocation\":\"Unknown\"," +
             "\"superPowers\":[\"Regeneritive Healing\",\"Enhanced Reflexes\",\"Adamantium-infused skeleton\",\"Retractable claws\"]," +
             "\"teamAffiliations\":[{\"name\":\"Avengers\"},{\"name\":\"X-Men\"}]," +
             "\"colorOfCostume\":\"Yellow\"," +
             "\"idNumber\":\"ID-65430987\"," +
             "\"dateOfLastCheckin\":\"12/01/2014\"," +
             "\"timeOfLastBattle\":\"09:43:23 21-08-2019\"," +
             "\"patrolStartTime\":\"20:00\"" +
            "}" +
           "]";
    }
}
