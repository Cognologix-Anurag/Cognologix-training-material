package com.cognologix.stream;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AdvanceStreamAPI {

    class Tuple2<T1, T2> implements Comparable<Tuple2<T1, T2>> {
        T1 t1;
        T2 t2;

        public Tuple2(T1 t1, T2 t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        @Override
        public int compareTo(Tuple2<T1, T2> anotherObject) {
            return ((Comparable) this.t1).compareTo(anotherObject.t1);
        }

        @Override
        public String toString() {
            return "{" +
                    "" + t1 +
                    ", " + t2 +
                    '}';
        }
    }

    @Test
    public void selectClause() {
        Stream.of(tuple(1, 1),
                tuple(2, 2)).forEach(System.out::println);
    }

    @Test
    public void crossJoin() {
        Stream<Integer> firstStream = Stream.of(1, 2);
        Supplier<Stream<String>> secondStreamSupplier = () -> Stream.of("A", "B");

        /*
            Perform cross join of 2 streams
            Hint : Need to use flatmap API.
         */
        
        Stream<Object> objectStream = firstStream.flatMap(first -> secondStreamSupplier.get()
                .map(second -> tuple(first, second)));
        System.out.println(objectStream);
        objectStream.forEach(System.out::println);

        /*
            Expected output
                {1, A}
                {1, B}
                {2, A}
                {2, B}
        */
    }

    @Test
    public void crossJoinWithList() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3);
        List<String> listOfStrings = Arrays.asList("A", "B", "C");

        /*
            Perform cross join of 2 streams
            Hint : Need to use flatmap API.
         */
        
         Stream<Object> objectStream = listOfInteger.stream()
                .flatMap(integer -> listOfStrings.stream()
                        .map(string -> tuple(integer, string)));
        objectStream.forEach(System.out::println);

        /*
        Expected output
            {1, A}
            {1, B}
            {1, C}
            {2, A}
            {2, B}
            {2, C}
            {3, A}
            {3, B}
            {3, C}
         */
    }

    @Test
    public void limit() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

        /*
            Use limit API
            limit the string elements to 5
         */
        
         Stream<Integer> limit = listOfInteger.stream()
                .limit(5);
        limit.forEach(System.out::println);

        /*
                1
                2
                3
                4
                5
         */
    }

    @Test
    public void offset() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

        Stream<Integer> skip = listOfInteger.stream()
                .skip(3);

        skip.forEach(System.out::println);

        /*
            Use skip API
            Skip first 3 elements of the stream and print rest of the elements

            Expected output:-

            4
            5
            6
            7
            8
            9
            10
            11
            12
            13
            14
            15
         */
    }

    @Test
    public void limitOffset() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

        /**
         * Note order of limit offset is also important.
         */
        listOfInteger
                .stream()
                .skip(3)
                .limit(5)
                .forEach(integer -> System.out.println(integer));

        /*
            Use Skip and limit API
            Skip First 3 elements then print only 5 elements in the string
            4
            5
            6
            7
            8
         */




    }

    @Test
    public void limitOffsetOrderIsImportant() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        
        Stream<Integer> limit = listOfInteger.stream()
                .skip(3)
                .limit(2);
        limit.forEach(System.out::println);

        /*
            Use limit,skip API.
            print below output
            Expected output :
                4
                5
         */
    }

    @Test
    public void groupByWithClassifier() {
        Stream<Object> stream = Stream.of(tuple(1, "Adam"),
                tuple(1, "Barn"),
                tuple(2, "Casilo"),
                tuple(2, "Sergio"),
                tuple(3, "Mikel"));

    // Have some doubt
        /*
            print below lines :-
            Use Group By collector
            1 [{1, Adam}, {1, Barn}]
            2 [{2, Casilo}, {2, Sergio}]
            3 [{3, Mikel}]
         */

    }



    @Test
    public void orderByWithComparator() {
        Stream<Object> stream = Stream.of(tuple(1, "Adam"),
                tuple(1, "Barn"),
                tuple(2, "Casilo"),
                tuple(2, "Sergio"),
                tuple(3, "Mikel"));
        
        // Have some doubt
             Stream<Object> sorted = stream.sorted();
        sorted.forEach(System.out::println);

        /*
            use sorted API
            Expected output

            {1, Adam}
            {1, Barn}
            {2, Casilo}
            {2, Sergio}
            {3, Mikel}

         */
    }


    @Test
    public void orderByReverseOrder() {
        Stream<Object> stream = Stream.of(tuple(1, "Adam"),
                tuple(1, "Barn"),
                tuple(2, "Casilo"),
                tuple(2, "Sergio"),
                tuple(3, "Mikel"));


        Stream<Object> sorted = stream.sorted(Collections.reverseOrder());
        sorted.forEach(System.out::println);

        /*
            Hint:- reverse ordering

            Expected output :-
            {3, Mikel}
            {2, Casilo}
            {2, Sergio}
            {1, Adam}
            {1, Barn}

         */
    }


    private <T1, T2> Object tuple(T1 t1, T2 t2) {
        return new Tuple2(t1, t2);
    }

}
