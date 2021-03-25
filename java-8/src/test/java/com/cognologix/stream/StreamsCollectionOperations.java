package com.cognologix.stream;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StreamsCollectionOperations {

    class Person {
        String name;
        int age;

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Test
    public void filterAndCollect() {

        /*
            Filter all persons having name starting with P and collect them in new List.
         */

        List<Person> persons =
                Arrays.asList(
                        new Person("Max", 18),
                        new Person("Peter", 23),
                        new Person("Pamela", 23),
                        new Person("David", 12));
    }

    @Test
    public void groupByAge() {

        /*
            Collect all persons group by age.
            Need to collect output in Map

             age 18: [Max]
             age 23: [Peter, Pamela]
             age 12: [David]
         */

        List<Person> persons =
                Arrays.asList(
                        new Person("Max", 18),
                        new Person("Peter", 23),
                        new Person("Pamela", 23),
                        new Person("David", 12));

        /*
            Expected output

            age 18: [Max]
             age 23: [Peter, Pamela]
             age 12: [David]
         */

    }

    @Test
    public void calculateAverageAge() {
        /*
            Calculate average age of the persons.
         */
        List<Person> persons =
                Arrays.asList(
                        new Person("Max", 18),
                        new Person("Peter", 23),
                        new Person("Pamela", 23),
                        new Person("David", 12));


    }
}
