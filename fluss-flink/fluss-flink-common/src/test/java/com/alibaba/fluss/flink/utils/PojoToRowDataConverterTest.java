package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.flink.common.Order;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class PojoToRowDataConverterTest {

    @Test
    public void testBasicConversion() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField(
                                        "address", new StringType(true), "Shipping address")));

        PojoToRowDataConverter<Order> converter =
                new PojoToRowDataConverter<>(Order.class, rowType);

        Order order = new Order(1001L, 5001L, 10, "123 Mumbai");

        RowData result = converter.convert(order);

        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getLong(1)).isEqualTo(5001L);
        assertThat(result.getInt(2)).isEqualTo(10);
        assertThat(result.getString(3).toString()).isEqualTo("123 Mumbai");
    }

    @Test
    public void testNullHandling() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField(
                                        "address", new StringType(true), "Shipping address")));

        PojoToRowDataConverter<Order> converter =
                new PojoToRowDataConverter<>(Order.class, rowType);

        RowData nullResult = converter.convert(null);
        assertThat(nullResult).isNull();

        Order order = new Order(1002L, 5002L, 5, null);
        RowData result = converter.convert(order);

        assertThat(result.getLong(0)).isEqualTo(1002L);
        assertThat(result.getLong(1)).isEqualTo(5002L);
        assertThat(result.getInt(2)).isEqualTo(5);
        assertThat(result.isNullAt(3)).isTrue();
    }

    @Test
    public void testMissingFields() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("address", new StringType(true), "Shipping address"),
                                new DataField(
                                        "nonExistentField",
                                        new StringType(true),
                                        "Non-existent field")));

        PojoToRowDataConverter<Order> converter =
                new PojoToRowDataConverter<>(Order.class, rowType);

        Order order = new Order(1003L, 5003L, 15, "456 Shenzhen");

        RowData result = converter.convert(order);

        assertThat(result.getArity()).isEqualTo(5);
        assertThat(result.getLong(0)).isEqualTo(1003L);
        assertThat(result.getLong(1)).isEqualTo(5003L);
        assertThat(result.getInt(2)).isEqualTo(15);
        assertThat(result.getString(3).toString()).isEqualTo("456 Shenzhen");
        assertThat(result.isNullAt(4)).isTrue();
    }

    @Test
    public void testFieldOrderIndependence() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("address", new StringType(true), "Shipping address"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID")));

        PojoToRowDataConverter<Order> converter =
                new PojoToRowDataConverter<>(Order.class, rowType);

        Order order = new Order(1004L, 5004L, 20, "789 Greece");

        RowData result = converter.convert(order);

        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getString(0).toString()).isEqualTo("789 Greece");
        assertThat(result.getInt(1)).isEqualTo(20);
        assertThat(result.getLong(2)).isEqualTo(1004L);
        assertThat(result.getLong(3)).isEqualTo(5004L);
    }

    @Test
    public void testComplexType() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("id", new BigIntType(false), "ID"),
                                new DataField("price", new DecimalType(false, 10, 2), "Price")));

        // Create a test class with a decimal field
        class ProductWithPrice {
            private long id;
            private BigDecimal price;

            public ProductWithPrice(long id, BigDecimal price) {
                this.id = id;
                this.price = price;
            }
        }

        PojoToRowDataConverter<ProductWithPrice> converter =
                new PojoToRowDataConverter<>(ProductWithPrice.class, rowType);

        ProductWithPrice product = new ProductWithPrice(1001L, new BigDecimal("99.99"));

        RowData result = converter.convert(product);

        assertThat(result.getArity()).isEqualTo(2);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getDecimal(1, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("99.99"));
    }

    @Test
    public void testInheritance() throws Exception {
        class Parent {
            protected String parentField;

            public Parent(String parentField) {
                this.parentField = parentField;
            }
        }

        class Child extends Parent {
            private int childField;

            public Child(String parentField, int childField) {
                super(parentField);
                this.childField = childField;
            }
        }

        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField("parentField", new StringType(true), "Parent field"),
                                new DataField("childField", new IntType(false), "Child field")));

        PojoToRowDataConverter<Child> converter =
                new PojoToRowDataConverter<>(Child.class, rowType);

        Child child = new Child("Parent value", 42);

        RowData result = converter.convert(child);

        assertThat(result.getArity()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("Parent value");
        assertThat(result.getInt(1)).isEqualTo(42);
    }

    @Test
    public void testEmptySchema() throws Exception {

        RowType emptyRowType = new RowType(true, Collections.emptyList());

        PojoToRowDataConverter<Order> converter =
                new PojoToRowDataConverter<>(Order.class, emptyRowType);

        Order order = new Order(1005L, 5005L, 25, "Empty schema test");

        RowData result = converter.convert(order);

        assertThat(result.getArity()).isEqualTo(0);
    }

    /** Test class with various data types to test type conversion */
    public static class ComplexTypeOrder extends Order {
        private boolean booleanValue;
        private byte tinyintValue;
        private short smallintValue;
        private float floatValue;
        private double doubleValue;
        private BigDecimal decimalValue;
        private LocalDate dateValue;
        private LocalTime timeValue;
        private LocalDateTime timestampValue;
        private byte[] bytesValue;
        private char charValue;

        public ComplexTypeOrder() {
            super();
        }

        public ComplexTypeOrder(
                long orderId,
                long itemId,
                int amount,
                String address,
                boolean booleanValue,
                byte tinyintValue,
                short smallintValue,
                float floatValue,
                double doubleValue,
                BigDecimal decimalValue,
                LocalDate dateValue,
                LocalTime timeValue,
                LocalDateTime timestampValue,
                byte[] bytesValue,
                char charValue) {
            super(orderId, itemId, amount, address);
            this.booleanValue = booleanValue;
            this.tinyintValue = tinyintValue;
            this.smallintValue = smallintValue;
            this.floatValue = floatValue;
            this.doubleValue = doubleValue;
            this.decimalValue = decimalValue;
            this.dateValue = dateValue;
            this.timeValue = timeValue;
            this.timestampValue = timestampValue;
            this.bytesValue = bytesValue;
            this.charValue = charValue;
        }
    }

    @Test
    void testConvertAllDataTypes() throws Exception {
        RowType rowType =
                new RowType(
                        true,
                        Arrays.asList(
                                // Basic Order fields
                                new DataField("orderId", new BigIntType(false), "Order ID"),
                                new DataField("itemId", new BigIntType(false), "Item ID"),
                                new DataField("amount", new IntType(false), "Order amount"),
                                new DataField("address", new StringType(true), "Shipping address"),
                                // Additional data types
                                new DataField(
                                        "booleanValue", new BooleanType(false), "Boolean value"),
                                new DataField(
                                        "tinyintValue", new TinyIntType(false), "TinyInt value"),
                                new DataField(
                                        "smallintValue", new SmallIntType(false), "SmallInt value"),
                                new DataField("floatValue", new FloatType(false), "Float value"),
                                new DataField("doubleValue", new DoubleType(false), "Double value"),
                                new DataField(
                                        "decimalValue",
                                        new DecimalType(false, 10, 2),
                                        "Decimal value"),
                                new DataField("dateValue", new DateType(false), "Date value"),
                                new DataField("timeValue", new TimeType(false, 3), "Time value"),
                                new DataField(
                                        "timestampValue",
                                        new TimestampType(false, 6),
                                        "Timestamp value"),
                                new DataField("bytesValue", new BinaryType(5), "Binary value"),
                                new DataField("charValue", new CharType(false, 1), "Char value")));

        // Create a ComplexTypeOrder with all fields
        ComplexTypeOrder order =
                new ComplexTypeOrder(
                        1001L,
                        5001L,
                        10,
                        "123 Mumbai",
                        true, // boolean
                        (byte) 127, // tinyint
                        (short) 32767, // smallint
                        3.14f, // float
                        2.71828, // double
                        new BigDecimal("123.45"), // decimal
                        LocalDate.of(2023, 7, 15), // date
                        LocalTime.of(14, 30, 45, 123000000), // time
                        LocalDateTime.of(2023, 7, 15, 14, 30, 45, 123456000), // timestamp
                        new byte[] {1, 2, 3, 4, 5}, // binary
                        'A' // char
                        );

        PojoToRowDataConverter<ComplexTypeOrder> converter =
                new PojoToRowDataConverter<>(ComplexTypeOrder.class, rowType);
        RowData result = converter.convert(order);

        assertThat(result.getArity()).isEqualTo(15);
        assertThat(result.getLong(0)).isEqualTo(1001L);
        assertThat(result.getLong(1)).isEqualTo(5001L);
        assertThat(result.getInt(2)).isEqualTo(10);
        assertThat(result.getString(3).toString()).isEqualTo("123 Mumbai");

        // Additional data types
        assertThat(result.getBoolean(4)).isTrue();
        assertThat(result.getByte(5)).isEqualTo((byte) 127);
        assertThat(result.getShort(6)).isEqualTo((short) 32767);
        assertThat(result.getFloat(7)).isEqualTo(3.14f);
        assertThat(result.getDouble(8)).isEqualTo(2.71828);
        assertThat(result.getDecimal(9, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));

        LocalDate expectedDate = LocalDate.of(2023, 7, 15);
        int expectedEpochDays = (int) expectedDate.toEpochDay();
        assertThat(result.getInt(10)).isEqualTo(expectedEpochDays);

        LocalTime expectedTime = LocalTime.of(14, 30, 45, 123000000);
        int expectedMillisOfDay = (int) (expectedTime.toNanoOfDay() / 1_000_000);
        assertThat(result.getInt(11)).isEqualTo(expectedMillisOfDay);

        LocalDateTime expectedTimestamp = LocalDateTime.of(2023, 7, 15, 14, 30, 45, 123456000);
        long expectedEpochMillis =
                expectedTimestamp.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L + 123L;
        assertThat(result.getTimestamp(12, 6).getMillisecond()).isEqualTo(expectedEpochMillis);

        assertThat(result.getBinary(13)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(result.getString(14).toString()).isEqualTo("A");
    }
}
