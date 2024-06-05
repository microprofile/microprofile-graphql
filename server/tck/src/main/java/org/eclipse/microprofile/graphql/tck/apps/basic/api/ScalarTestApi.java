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
package org.eclipse.microprofile.graphql.tck.apps.basic.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import javax.json.bind.annotation.JsonbProperty;

import org.eclipse.microprofile.graphql.DateFormat;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Id;
import org.eclipse.microprofile.graphql.Mutation;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.NonNull;
import org.eclipse.microprofile.graphql.NumberFormat;
import org.eclipse.microprofile.graphql.Query;

/**
 * Scalar testing.
 * This allows basic testing of the scalar mapping as defined in GraphQL Entities-Scalars section.
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
@GraphQLApi
public class ScalarTestApi {
    @Query("testScalarsInPojo")
    public ScalarHolder getScalarHolder() {
        ScalarHolder sh = new ScalarHolder();

        // Short
        short s = 123;
        sh.setShortObject(new Short(s));
        sh.setFormattedShortObject(new Short(s));
        sh.setShortPrimitive(s);

        // Integer
        int i = 123456789;
        sh.setIntObject(new Integer(i));
        sh.setFormattedIntObject(new Integer(i));
        sh.setIntPrimitive(i);

        // Long
        long l = 123456789L;
        sh.setLongObject(new Long(l));
        sh.setFormattedLongObject(new Long(l));
        sh.setLongPrimitive(l);
        sh.setFormattedLongPrimitive(l);

        // Float
        float f = 123456.789F;
        sh.setFloatObject(new Float(f));
        sh.setFormattedFloatObject(new Float(f));
        sh.setFloatPrimitive(f);

        // Double
        double d = 123456.789;
        sh.setDoubleObject(new Double(d));
        sh.setFormattedDoubleObject(new Double(d));
        sh.setDoublePrimitive(d);

        // Byte
        byte b = 123;
        sh.setByteObject(new Byte("123"));
        sh.setFormattedByteObject(new Byte("123"));
        sh.setBytePrimitive(b);

        // BigInteger
        sh.setBigIntegerObject(BigInteger.valueOf(123456789));
        sh.setFormattedBigIntegerObject(BigInteger.valueOf(123456789));

        // BigDecimal
        sh.setBigDecimalObject(BigDecimal.valueOf(123456.789));
        sh.setFormattedBigDecimalObject(BigDecimal.valueOf(123456.789));

        // Boolean
        sh.setBooleanObject(Boolean.valueOf(false));
        sh.setBooleanPrimitive(false);

        // Character
        char c = "c".charAt(0);
        sh.setCharObject(new Character(c));
        sh.setCharPrimitive(c);

        // String
        sh.setStringObject("123456789");
        sh.setCharArray("123456789".toCharArray());

        // LocalDate
        LocalDate localDate = LocalDate.parse("2019-10-23");
        sh.setDateObject(localDate);
        sh.setAnotherDateObject(localDate);
        sh.setFormattedDateObject(localDate);

        // LocalTime
        LocalTime localTime = LocalTime.parse("11:46:34.263");
        sh.setTimeObject(localTime);
        sh.setAnotherTimeObject(localTime);
        sh.setFormattedTimeObject(localTime);

        // LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.parse("2019-10-23T11:46:34.263");
        sh.setDateTimeObject(localDateTime);
        sh.setAnotherDateTimeObject(localDateTime);
        sh.setFormattedDateTimeObject(localDateTime);

        // ID
        sh.setId("123456789");

        return sh;
    }

    @Mutation
    public ScalarHolder setScalarHolder(ScalarHolder scalarHolder) {
        return scalarHolder;
    }

    @Query
    public short getShortPrimitive(){
        return getScalarHolder().getShortPrimitive();
    }
    @Query
    public Short getShortObject(){
        return getScalarHolder().getShortPrimitive();
    }

    @Query
    @JsonbProperty("testIntPrimitive")
    public int intPrimitive(){
        return getScalarHolder().getIntPrimitive();
    }

    @Query
    @Name("testIntObject")
    public Integer intObject(){
        return getScalarHolder().getIntObject();
    }

    @Query
    @JsonbProperty("testLongPrimitive")
    public long longPrimitive(){
        return getScalarHolder().getLongPrimitive();
    }

    @Query
    @Name("testLongObject")
    public Long longObject(){
        return getScalarHolder().getLongObject();
    }

    @Query
    @JsonbProperty("testFloatPrimitive")
    public float floatPrimitive(){
        return getScalarHolder().getFloatPrimitive();
    }

    @Query
    @Name("testFloatObject")
    public Float floatObject(){
        return getScalarHolder().getFloatObject();
    }

    @Query
    @JsonbProperty("testDoublePrimitive")
    public double doublePrimitive(){
        return getScalarHolder().getDoublePrimitive();
    }

    @Query
    @Name("testDoubleObject")
    public Double doubleObject(){
        return getScalarHolder().getDoubleObject();
    }

    @Query
    public boolean isBooleanPrimitive(){
        return getScalarHolder().isBooleanPrimitive();
    }
    @Query
    public Boolean isBooleanObject(){
        return getScalarHolder().getBooleanObject();
    }

    @Query
    @JsonbProperty("testCharPrimitive")
    public char charPrimitive(){
        return getScalarHolder().getCharPrimitive();
    }

    @Query
    @Name("testCharObject")
    public Character charObject(){
        return getScalarHolder().getCharObject();
    }

    @Query
    @JsonbProperty("testCharArray")
    public char[] charArray(){
        return getScalarHolder().getCharArray();
    }
    @Query
    @Name("testStringObject")
    public String stringObject(){
        return getScalarHolder().getStringObject();
    }

    @Query
    @JsonbProperty("testBytePrimitive")
    public byte bytePrimitive(){
        return getScalarHolder().getBytePrimitive();
    }
    @Query
    @Name("testByteObject")
    public Byte byteObject(){
        return getScalarHolder().getByteObject();
    }

    @Query
    @JsonbProperty("testBigIntegerObject")
    public BigInteger bigIntegerObject(){
        return getScalarHolder().getBigIntegerObject();
    }

    @Query
    @Name("testBigDecimalObject")
    public BigDecimal bigDecimalObject(){
        return getScalarHolder().getBigDecimalObject();
    }

    @Query
    @JsonbProperty("testDateObject")
    public LocalDate dateObject(){
        return getScalarHolder().getDateObject();
    }

    @Query
    @Name("testTimeObject")
    public LocalTime timeObject(){
        return getScalarHolder().getTimeObject();
    }

    @Query
    @JsonbProperty("testDateTimeObject")
    public LocalDateTime dateTimeObject(){
        return getScalarHolder().getDateTimeObject();
    }

    @Query @Id
    @Name("testId")
    public String id(){
        return getScalarHolder().getId();
    }

    @Query
    public BasicInterface basicMessageEcho(@Name("input") BasicInput input) {
        return new BasicType(input.getMessage());
    }

    @Query
    public String getaway(){
        return "Just testing a name that starts with get but is not a getter";
    }

    @Mutation
    public String settlement(){
        return "Just testing a name that starts with set but is not a setter";
    }

    @Query
    public String testNonNullParameter(@Name("param") @NonNull String param) {
        return param;
    }

    @Query
    @Description("Testing transformed date as a response")
    @DateFormat(value = "dd MMM yyyy", locale = "en-GB")
    public LocalDate transformedDate(){
        String date = "2016-08-16";
        return LocalDate.parse(date);
    }

    @Mutation
    @Description("Testing transformed number as a response")
    @NumberFormat(value = "number #", locale = "en-GB")
    public Integer transformedNumber(Integer input){
        return input;
    }

    @Mutation
    public Person addPerson(Person person) {
        return person;
    }

    @Mutation
    public JsonbPerson addJsonbPerson(JsonbPerson person) {
        return person;
    }

    @Query
    public BasicUnion basicUnionSelection(@Name("name") String name, @Name("count") Integer count) {
        if (name != null) {
            return new UnionMember(name);
        }
        return new AnotherUnionMember(count == null ? 0 : count);
    }

}
