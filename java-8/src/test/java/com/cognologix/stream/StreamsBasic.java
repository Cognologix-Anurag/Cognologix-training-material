package com.cognologix.stream;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StreamsBasic {

    @Test
    public void createStreamFromArraysAndPrintOnConsole() {
        //List<String> myList = <write >
        List<String> myList;
    }

    @Test
    public void convertToUpperCase() {
        List<String> myList =
                Arrays.asList("a1", "a2", "b1", "c2", "c1");

        //write your code here - convert all elements to upper case
        List<String> outputListUpper = Arrays.asList("A1", "A2", "B1", "C2", "C1");
        List<String> myListUpper = myList.stream()
                .map(ele -> ele.toUpperCase())
                .collect(Collectors.toList());
        assertEquals(outputListUpper, myListUpper);
    }

    @Test
    public void printTheFirstElement() {
        List<String> myList =
                Arrays.asList("a1", "a2", "b1", "c2", "c1");

        //write your code here - print the first element.
        Optional<String> first = myList.stream()
                .findFirst();
        assertEquals("a1", first.get());
        
    }

    @Test
    public void sortTheStream() {
        List<String> myList =
                Arrays.asList("a1", "a2", "b1", "c2", "c1");

        //write your code here - sort the above array.
        List<String> sortedList = myList.stream()
                .sorted()
                .collect(Collectors.toList());

        assertEquals(sortedList.get(0), "a1");
        assertEquals(sortedList.get(4), "c2");
    }

    @Test
    public void filterElementsFromTheStream() {
        List<String> myList =
                Arrays.asList("a1", "a2", "b1", "c2", "c1");

        //filter c1 element
        List<String> only_c1 = myList.stream()
                .filter(ele -> ele.contains("c1"))
                .collect(Collectors.toList());
        assertEquals("c1", only_c1.get(0));
    }

    @Test
    public void convertLinesToWords() {
        String string = "The history of New York begins around 10,000 BC, when the first Native Americans arrived. By 1100 AD, New York's main native cultures, the Iroquoian and Algonquian, had developed. European discovery of New York was led by the French in 1524 and the first land claim came in 1609 by the Dutch. As part of New Netherland, the colony was important in the fur trade and eventually became an agricultural resource thanks to the patroon system. In 1626 the Dutch bought the island of Manhattan from Native Americans.[1] In 1664, England renamed the colony New York, after the Duke of York (later James II & VII.) New York City gained prominence in the 18th century as a major trading port in the Thirteen Colonies.\r\n" +
                "New York played a pivotal role during the American Revolution and subsequent war. The Stamp Act Congress in 1765 brought together representatives from across the Thirteen Colonies to form a unified response to British policies. The Sons of Liberty were active in New York City to challenge British authority. After a major loss at the Battle of Long Island, the Continental Army suffered a series of additional defeats that forced a retreat from the New York City area, leaving the strategic port and harbor to the British army and navy as their North American base of operations for the rest of the war. The Battle of Saratoga was the turning point of the war in favor of the Americans, convincing France to formally ally with them. New York's constitution was adopted in 1777, and strongly influenced the United States Constitution. New York City was the national capital at various times between 1785 and 1790, where the Bill of Rights was drafted. Albany became the permanent state capital in 1797. In 1787, New York became the eleventh state to ratify the United States Constitution.";

        /*
            you can use below method to convert into streams
            Arrays.stream(string.split("\r\n"))
         */
        
        // Have some doubt
        
        /*
            Expected output :-
                The
                history
                of
                New
                York
                begins
                around
                10,000
                BC,
                when
        */
    }
}
