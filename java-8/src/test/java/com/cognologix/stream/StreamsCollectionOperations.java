package com.cognologix.stream;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

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

        public String getAgeStr(){
            return "age "+ String.valueOf(age);
        }

        public int getAge(){
            return age;
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

        List p_persons=persons.stream().filter(x->x.name.startsWith("P")).collect(Collectors.toList());

        System.out.println(p_persons);
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

        Map<String,List<String>> age_grp_by=persons.stream().collect(
                Collectors.groupingBy(Person::getAgeStr,
                        Collectors.mapping(Person::toString,Collectors.toList())
                )
        );

        System.out.println(age_grp_by);

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

        OptionalDouble avg_age=persons.stream().mapToInt(Person::getAge).average();
        System.out.println(avg_age);
    }
}
