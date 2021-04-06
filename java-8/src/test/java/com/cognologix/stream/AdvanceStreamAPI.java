package com.cognologix.stream;

import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AdvanceStreamAPI {

    class Tuple2<T1, T2> implements Comparable<Tuple2<T1, T2>> {
        T1 t1;
        T2 t2;

        public Tuple2(T1 t1, T2 t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        public T1 getT1() {
            return t1;
        }

        public T2 getT2() {
            return t2;
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

        /*
            Expected output
                {1, A}
                {1, B}
                {2, A}
                {2, B}
        */

        firstStream.flatMap(i->secondStreamSupplier.get().map(j->tuple(i,j))).forEach(System.out::println);
    }

    @Test
    public void crossJoinWithList() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3);
        List<String> listOfStrings = Arrays.asList("A", "B", "C");

        /*
            Perform cross join of 2 streams
            Hint : Need to use flatmap API.
         */

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

        listOfInteger.stream().flatMap(i->listOfStrings.stream().map(j->tuple(i,j))).forEach(System.out::println);
    }

    @Test
    public void limit() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

        /*
            Use limit API
            limit the string elements to 5
         */

        /*
                1
                2
                3
                4
                5
         */

        listOfInteger.stream().limit(5).forEach(System.out::println);
    }

    @Test
    public void offset() {
        List<Integer> listOfInteger = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);



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

        listOfInteger.stream().skip(3).forEach(System.out::println);
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
                .forEach(integer -> System.out.println(integer))
        ;

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

        /*
            Use limit,skip API.
            print below output
            Expected output :
                4
                5
         */
        listOfInteger.stream().skip(3).limit(2).forEach(System.out::println);
    }

    @Test
    public void groupByWithClassifier() {
        Stream<Object> stream = Stream.of(tuple(1, "Adam"),
                tuple(1, "Barn"),
                tuple(2, "Casilo"),
                tuple(2, "Sergio"),
                tuple(3, "Mikel"));


        /*
            print below lines :-
            Use Group By collector
            1 [{1, Adam}, {1, Barn}]
            2 [{2, Casilo}, {2, Sergio}]
            3 [{3, Mikel}]
         */

        Map<Integer, List<Object>> collect = stream.map(o -> ((Tuple2<Integer,String>)o)).
                collect(Collectors.groupingBy(integerStringTuple2 -> ((Tuple2<Integer,String>)integerStringTuple2).t1));
    }



    @Test
    public void orderByWithComparator() {
        Stream<Object> stream = Stream.of(tuple(1, "Adam"),
                tuple(1, "Barn"),
                tuple(2, "Casilo"),
                tuple(2, "Sergio"),
                tuple(3, "Mikel"));

        /*
            use sorted API
            Expected output

            {1, Adam}
            {1, Barn}
            {2, Casilo}
            {2, Sergio}
            {3, Mikel}

         */

        List<Object> sortedTuple=stream.sorted((t1,t2)->((Tuple2<Integer,String>)t1).compareTo((Tuple2<Integer,String>)t2)).collect(Collectors.toList());

        System.out.println(sortedTuple);
    }


    @Test
    public void orderByReverseOrder() {
        Stream<Object> stream = Stream.of(tuple(1, "Adam"),
                tuple(1, "Barn"),
                tuple(2, "Casilo"),
                tuple(2, "Sergio"),
                tuple(3, "Mikel"));



        /*
            Hint:- reverse ordering

            Expected output :-
            {3, Mikel}
            {2, Casilo}
            {2, Sergio}
            {1, Adam}
            {1, Barn}

         */

        List<Object> reverseTuple = stream.sorted(Comparator.comparing(t->((Tuple2<Integer,String>)t).t1).reversed()).collect(Collectors.toList());

        System.out.println(reverseTuple);
    }


    private <T1, T2> Object tuple(T1 t1, T2 t2) {
        return new Tuple2(t1, t2);
    }

}
