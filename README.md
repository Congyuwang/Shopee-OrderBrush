# OrderBrushing

## Shopee

This is related to a Shopee competition.
It mainly consists of a OrderBrushOrder class written in java
that accepts transaction records as new transaction occurs
and returns suspicious order brushing users and shops.

The project also has a python version of a slightly different
implementation using numpy library.

The java version has a score of 0.99508. 
It processes 222750 transactions within 6 seconds on a 1.6 GHz processor.

The python version has a score of 0.89933.

## orderBrushing.DetectOrderBrushing class

Only memorize the recent one hour orders,
and thus is quite memory efficient,
and has constant processing on average for processing each
new transaction.
