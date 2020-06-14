# OrderBrush

## Shopee

This is related to a Shopee competition.
It mainly consists of a OrderBrushOrder class written in java
that accepts transaction records as new transaction occurs
and returns suspicious order brushing users and shops.

The project also has a python version of a slightly different
implementation using numpy library.

The java version has a score of 99.508%.

The python version has a score of 89.933%.

## OrderBrushOrder

Only memorize the recent one hour orders,
and thus is quite memory efficient,
and has constant processing on average for processing each
new transaction.
