package com.bm.springbatch;

/**
 * @PROJECT IntelliJ IDEA
 * @AUTHOR Bikash Mainali
 * @DATE 6/1/24
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {

        String s = "California continued to grow rapidly and soon became an agricultural and industrial power. " +
                "The economy was widely based on specialty agriculture";

        String[] splitedStr = s.split("\\s");

        StringBuilder result = new StringBuilder();

        for(String str : splitedStr) {
            String reversedWord = new StringBuilder(str).reverse().toString();
            result.append(reversedWord).append(" ");
        }

        System.out.println(result.toString().trim());

        Map<Character, Integer> characterCountMap = new HashMap<>();

        for(char c : result.toString().toCharArray()) {
            char lowerChar = Character.toLowerCase(c);
            if(lowerChar != ' ') {
                if (characterCountMap.containsKey(lowerChar)) {
                    characterCountMap.put(lowerChar, characterCountMap.get(lowerChar) + 1);
                } else {
                    characterCountMap.put(lowerChar, 1);
                }
            }
        }

        System.out.println(characterCountMap);

    }
}
