package com.bm.springbatch;

/**
 * @PROJECT IntelliJ IDEA
 * @AUTHOR Bikash Mainali
 * @DATE 6/1/24
 */

import org.springframework.batch.item.ItemProcessor;

public class PaymentItemProcessor implements ItemProcessor<Payment, Payment> {

    public Payment process(Payment item) {
        System.out.println("Processing..." + item);
        return item;
    }

}
