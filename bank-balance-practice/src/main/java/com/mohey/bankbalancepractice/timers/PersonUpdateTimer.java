package com.mohey.bankbalancepractice.timers;

import com.mohey.bankbalancepractice.producers.UsersTransactions;
import java.util.Date;
import java.util.Random;
import java.util.TimerTask;

public class PersonUpdateTimer extends TimerTask {



    Random random = new Random();
    @Override
    public void run() {
        UsersTransactions.johnUser.setAmount(random.nextInt(1000));
        UsersTransactions.johnUser.setTime(new Date());

        UsersTransactions.moheyUser.setAmount(random.nextInt(1000));
        UsersTransactions.moheyUser.setTime(new Date());

        UsersTransactions.mahmoudUser.setAmount(random.nextInt(1000));
        UsersTransactions.mahmoudUser.setTime(new Date());

        UsersTransactions.ahmedUser.setAmount(random.nextInt(1000));
        UsersTransactions.ahmedUser.setTime(new Date());

        UsersTransactions.badrUser.setAmount(random.nextInt(1000));
        UsersTransactions.badrUser.setTime(new Date());

        UsersTransactions.zakiUser.setAmount(random.nextInt(1000));
        UsersTransactions.zakiUser.setTime(new Date());



    }
}
