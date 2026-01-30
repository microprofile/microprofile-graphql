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
package org.eclipse.microprofile.graphql.tck.apps.basic.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import org.eclipse.microprofile.graphql.DefaultValue;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.Id;

import jakarta.json.bind.annotation.JsonbDateFormat;
import jakarta.json.bind.annotation.JsonbNumberFormat;

/**
 * Plain POJO with scalar fields
 *
 * @author Phillip Kruger (phillip.kruger@redhat.com)
 */
public class ScalarHolder {
    // Short
    private short shortPrimitive;
    private Short shortObject;
    @JsonbNumberFormat(value = "¤000", locale = "en-ZA")
    private Short formattedShortObject;

    // Integer
    private int intPrimitive;
    private Integer intObject;
    @JsonbNumberFormat(locale = "en-US")
    private Integer formattedIntObject;

    // Long
    private long longPrimitive;
    @JsonbNumberFormat(locale = "is-IS")
    private long formattedLongPrimitive;
    private Long longObject;
    @JsonbNumberFormat(locale = "is-IS")
    private Long formattedLongObject;

    // Float
    private float floatPrimitive;
    private Float floatObject;
    @JsonbNumberFormat(value = "#0.0", locale = "en-GB")
    private Float formattedFloatObject;

    // Double
    private double doublePrimitive;
    private Double doubleObject;
    @Description("This is a formatted number")
    @JsonbNumberFormat(value = "#0.0", locale = "en-GB")
    private Double formattedDoubleObject;

    // Byte
    private byte bytePrimitive;
    private Byte byteObject;
    @JsonbNumberFormat(value = "¤00", locale = "en-ZA")
    private Byte formattedByteObject;

    // BigInteger
    private BigInteger bigIntegerObject;
    @JsonbNumberFormat(value = "¤000", locale = "en-US")
    private BigInteger formattedBigIntegerObject;

    // BigDecimal
    private BigDecimal bigDecimalObject;
    @JsonbNumberFormat(value = "#,###.##", locale = "en-GB")
    private BigDecimal formattedBigDecimalObject;

    // Boolean
    private boolean booleanPrimitive;
    private Boolean booleanObject;

    // Character
    private char charPrimitive;
    private Character charObject;

    // String
    private char[] charArray;
    private String stringObject;

    // LocalDate
    @DefaultValue("1978-07-03")
    private LocalDate dateObject;
    @Description("This is another date")
    private LocalDate anotherDateObject;
    @JsonbDateFormat("MM dd yyyy")
    @Description("This is a formatted date")
    private LocalDate formattedDateObject;

    // LocalTime
    private LocalTime timeObject;
    @Description("This is another time")
    private LocalTime anotherTimeObject;
    @JsonbDateFormat("hh:mm:ss")
    @Description("This is a formatted time")
    private LocalTime formattedTimeObject;

    // LocalDateTime
    private LocalDateTime dateTimeObject;
    @Description("This is another datetime")
    private LocalDateTime anotherDateTimeObject;
    @JsonbDateFormat("MM dd yyyy 'at' hh:mm:ss")
    @Description("This is a formatted datetime")
    private LocalDateTime formattedDateTimeObject;

    // ID
    @Id
    private String id;
    @Id
    private long longPrimitiveId;
    @Id
    private Long longObjectId;
    @Id
    private int intPrimitiveId;
    @Id
    private Integer integerObjectId;
    @Id
    private UUID uuidId;

    public ScalarHolder() {

    }

    public short getShortPrimitive() {
        return shortPrimitive;
    }

    public void setShortPrimitive(short shortPrimitive) {
        this.shortPrimitive = shortPrimitive;
    }

    public Short getShortObject() {
        return shortObject;
    }

    public void setShortObject(Short shortObject) {
        this.shortObject = shortObject;
    }

    public Short getFormattedShortObject() {
        return formattedShortObject;
    }

    public void setFormattedShortObject(Short formattedShortObject) {
        this.formattedShortObject = formattedShortObject;
    }

    public int getIntPrimitive() {
        return intPrimitive;
    }

    public void setIntPrimitive(int intPrimitive) {
        this.intPrimitive = intPrimitive;
    }

    public Integer getIntObject() {
        return intObject;
    }

    public void setIntObject(Integer intObject) {
        this.intObject = intObject;
    }

    public Integer getFormattedIntObject() {
        return formattedIntObject;
    }

    public void setFormattedIntObject(Integer formattedIntObject) {
        this.formattedIntObject = formattedIntObject;
    }

    public long getLongPrimitive() {
        return longPrimitive;
    }

    public void setLongPrimitive(long longPrimitive) {
        this.longPrimitive = longPrimitive;
    }

    public long getFormattedLongPrimitive() {
        return formattedLongPrimitive;
    }

    public void setFormattedLongPrimitive(long formattedLongPrimitive) {
        this.formattedLongPrimitive = formattedLongPrimitive;
    }

    public Long getLongObject() {
        return longObject;
    }

    public void setLongObject(Long longObject) {
        this.longObject = longObject;
    }

    public Long getFormattedLongObject() {
        return formattedLongObject;
    }

    public void setFormattedLongObject(Long formattedLongObject) {
        this.formattedLongObject = formattedLongObject;
    }

    public float getFloatPrimitive() {
        return floatPrimitive;
    }

    public void setFloatPrimitive(float floatPrimitive) {
        this.floatPrimitive = floatPrimitive;
    }

    public Float getFloatObject() {
        return floatObject;
    }

    public void setFloatObject(Float floatObject) {
        this.floatObject = floatObject;
    }

    public Float getFormattedFloatObject() {
        return formattedFloatObject;
    }

    public void setFormattedFloatObject(Float formattedFloatObject) {
        this.formattedFloatObject = formattedFloatObject;
    }

    public double getDoublePrimitive() {
        return doublePrimitive;
    }

    public void setDoublePrimitive(double doublePrimitive) {
        this.doublePrimitive = doublePrimitive;
    }

    public Double getDoubleObject() {
        return doubleObject;
    }

    public void setDoubleObject(Double doubleObject) {
        this.doubleObject = doubleObject;
    }

    public Double getFormattedDoubleObject() {
        return formattedDoubleObject;
    }

    public void setFormattedDoubleObject(Double formattedDoubleObject) {
        this.formattedDoubleObject = formattedDoubleObject;
    }

    public byte getBytePrimitive() {
        return bytePrimitive;
    }

    public void setBytePrimitive(byte bytePrimitive) {
        this.bytePrimitive = bytePrimitive;
    }

    public Byte getByteObject() {
        return byteObject;
    }

    public void setByteObject(Byte byteObject) {
        this.byteObject = byteObject;
    }

    public Byte getFormattedByteObject() {
        return formattedByteObject;
    }

    public void setFormattedByteObject(Byte formattedByteObject) {
        this.formattedByteObject = formattedByteObject;
    }

    public BigInteger getBigIntegerObject() {
        return bigIntegerObject;
    }

    public void setBigIntegerObject(BigInteger bigIntegerObject) {
        this.bigIntegerObject = bigIntegerObject;
    }

    public BigInteger getFormattedBigIntegerObject() {
        return formattedBigIntegerObject;
    }

    public void setFormattedBigIntegerObject(BigInteger formattedBigIntegerObject) {
        this.formattedBigIntegerObject = formattedBigIntegerObject;
    }

    public BigDecimal getBigDecimalObject() {
        return bigDecimalObject;
    }

    public void setBigDecimalObject(BigDecimal bigDecimalObject) {
        this.bigDecimalObject = bigDecimalObject;
    }

    public BigDecimal getFormattedBigDecimalObject() {
        return formattedBigDecimalObject;
    }

    public void setFormattedBigDecimalObject(BigDecimal formattedBigDecimalObject) {
        this.formattedBigDecimalObject = formattedBigDecimalObject;
    }

    public boolean isBooleanPrimitive() {
        return booleanPrimitive;
    }

    public void setBooleanPrimitive(boolean booleanPrimitive) {
        this.booleanPrimitive = booleanPrimitive;
    }

    public Boolean getBooleanObject() {
        return booleanObject;
    }

    public void setBooleanObject(Boolean booleanObject) {
        this.booleanObject = booleanObject;
    }

    public char getCharPrimitive() {
        return charPrimitive;
    }

    public void setCharPrimitive(char charPrimitive) {
        this.charPrimitive = charPrimitive;
    }

    public Character getCharObject() {
        return charObject;
    }

    public void setCharObject(Character charObject) {
        this.charObject = charObject;
    }

    public char[] getCharArray() {
        return charArray;
    }

    public void setCharArray(char[] charArray) {
        this.charArray = charArray;
    }

    public String getStringObject() {
        return stringObject;
    }

    public void setStringObject(String stringObject) {
        this.stringObject = stringObject;
    }

    public LocalDate getDateObject() {
        return dateObject;
    }

    public void setDateObject(LocalDate dateObject) {
        this.dateObject = dateObject;
    }

    public LocalDate getAnotherDateObject() {
        return anotherDateObject;
    }

    public void setAnotherDateObject(LocalDate anotherDateObject) {
        this.anotherDateObject = anotherDateObject;
    }

    public LocalDate getFormattedDateObject() {
        return formattedDateObject;
    }

    public void setFormattedDateObject(LocalDate formattedDateObject) {
        this.formattedDateObject = formattedDateObject;
    }

    public LocalTime getTimeObject() {
        return timeObject;
    }

    public void setTimeObject(LocalTime timeObject) {
        this.timeObject = timeObject;
    }

    public LocalTime getAnotherTimeObject() {
        return anotherTimeObject;
    }

    public void setAnotherTimeObject(LocalTime anotherTimeObject) {
        this.anotherTimeObject = anotherTimeObject;
    }

    public LocalTime getFormattedTimeObject() {
        return formattedTimeObject;
    }

    public void setFormattedTimeObject(LocalTime formattedTimeObject) {
        this.formattedTimeObject = formattedTimeObject;
    }

    public LocalDateTime getDateTimeObject() {
        return dateTimeObject;
    }

    public void setDateTimeObject(LocalDateTime dateTimeObject) {
        this.dateTimeObject = dateTimeObject;
    }

    public LocalDateTime getAnotherDateTimeObject() {
        return anotherDateTimeObject;
    }

    public void setAnotherDateTimeObject(LocalDateTime anotherDateTimeObject) {
        this.anotherDateTimeObject = anotherDateTimeObject;
    }

    public LocalDateTime getFormattedDateTimeObject() {
        return formattedDateTimeObject;
    }

    public void setFormattedDateTimeObject(LocalDateTime formattedDateTimeObject) {
        this.formattedDateTimeObject = formattedDateTimeObject;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getLongPrimitiveId() {
        return longPrimitiveId;
    }

    public void setLongPrimitiveId(long longPrimitiveId) {
        this.longPrimitiveId = longPrimitiveId;
    }

    public Long getLongObjectId() {
        return longObjectId;
    }

    public void setLongObjectId(Long longObjectId) {
        this.longObjectId = longObjectId;
    }

    public int getIntPrimitiveId() {
        return intPrimitiveId;
    }

    public void setIntPrimitiveId(int intPrimitiveId) {
        this.intPrimitiveId = intPrimitiveId;
    }

    public Integer getIntegerObjectId() {
        return integerObjectId;
    }

    public void setIntegerObjectId(Integer integerObjectId) {
        this.integerObjectId = integerObjectId;
    }

    public UUID getUuidId() {
        return uuidId;
    }

    public void setUuidId(UUID uuidId) {
        this.uuidId = uuidId;
    }

}
