"""
Created on Sun Jun 14 10:30:17 2020

@author: congyuwang
"""

import numpy as np


class Orderbrush:

    ONE_HOUR = 3600

    def __init__(self, transactionData: np.ndarray):
        """
        Orderbrush(transactionData) accepts a numpy ndarray with four columns:
            ``0:orderId, 1:shopId, 2:userId, 3:event_time``

        It returns returns a dictionary of shopId as key, and a tuple of suspicious
        users related to this shop as value. If the shop is not suspicious of
        brushing orders, the value is None.

        Attributes:
            ``ONE_HOUR``: is a constant that represents the number of seconds in an hour.

        Constructor:
            an instance of the class must be initialized with an ndarray of four columns.

        Method:
            ``get_suspicious_shop_users()`` returns a dictionary of shopId as key,
            and a tuple of suspicious users related to this shop as value.

        Example:
        ::

            transactionData = pd.read_csv("data.csv").to_numpy()
            Orderbrush ob = new Orderbrush(transactionData)
            suspicious_shops_and_users: dict = ob.get_suspicious_shop_users()

        """
        assert transactionData.shape[1] == 4
        self.__data: np.ndarray = transactionData.copy()
        self.__data = self.__data[np.argsort(self.__data[:, 3])]
        self.__UNIQUE_SHOP_ID: np.ndarray = np.unique(transactionData[:, 1])

    def __get_shop_transaction__(self, shopId: np.int64) -> np.ndarray:
        """
        Get the transactions of a particular shop

        Parameters
        ----------
        shopId : np.int64
            input shopId to look up its transaction records.

        Returns
        -------
        np.ndarray
            Three columns: 0:orderId, 1:userId and 2:event_time of a specific shop.

        """
        # get all transactions of the shop
        return self.__data[self.__data[:, 1] == shopId, :][:, [0, 2, 3]]

    def __single_concentration__(self, userId: np.ndarray) -> np.int64:
        """
        a simple function calculates the concentration of a userId array
        """
        if userId.shape[0] == 0:
            return 0.0
        return np.floor(userId.shape[0] / np.unique(userId).shape[0])

    def __concentration__(self, transactions: np.ndarray) -> np.ndarray:
        """
        calculate concentrations for every transaction of a shop


        Parameters
        ----------
        transactions : np.ndarray
            Transactions of a *SINGLE* shop.
            Three columns: 0:orderId, 1:userId and 2:event_time of a specific shop.

        Returns
        -------
        records : np.ndarray
            Returns an array of concentrations
            Four columns: 0:orderId, 1:userId, 2:concentration and 3:event_time.

        """
        assert transactions.shape[1] == 3
        event_time: np.ndarray = transactions[:, 2].flatten()
        length: int = transactions.shape[0]

        ## create result container
        concentrations: np.ndarray = np.zeros((length, 4), dtype=np.int64)

        i: int = 0
        for record in transactions:

            # starting from each transaction time t
            t: np.int64 = record[2]

            # calculate concentration rate starting from t to t + 1 hour
            nextHour = np.logical_and(
                event_time >= t, event_time <= (t + Orderbrush.ONE_HOUR))
            userId: np.ndarray = transactions[nextHour, 1]
            concentration_rate: np.int64 = self.__single_concentration__(
                userId)

            ## write 0:orderId, 1:userId, 2:concentration and 3:event_time
            concentrations[i] = [record[0], record[1], concentration_rate, t]
            i += 1

        return concentrations

    def __get_suspicious_orders__(self, transactions: np.ndarray) -> list:
        """
        get suspicious orders from the transaction records of a particular shop


        Parameters
        ----------
        transactions : np.ndarray
            Transactions of a *SINGLE* shop.
            Three columns: 0:orderId, 1:userId and 2:event_time of a specific shop.

        Returns
        -------
        list
            List of suspicious orders: (orderId, userId) tuples for a specific shop.

        """
        assert transactions.shape[1] == 3

        # get concentration (variable: cons)
        cons: np.ndarray = self.__concentration__(transactions)
        event_time: np.ndarray = cons[:, 3]

        # record suspicious_orders in a set
        suspicious_orders: set = set()

        # remember the last upper_time
        last_upper_time: np.int64 = 0

        # for each suspicious
        for row in range(cons.shape[0]):

            # if concentration reaches 3
            if cons[row, 2] >= 3:

                # get time period for suspicious transactions
                suspicious_time: np.int64 = event_time[row]
                upper_time: np.int64 = suspicious_time + Orderbrush.ONE_HOUR

                # skip if this time period covers the previous time period
                if suspicious_time <= last_upper_time:
                    continue

                # get (orderId, userId) of suspicious time period
                bad_orders: np.ndarray = cons[np.logical_and(
                    suspicious_time <= event_time, event_time <= upper_time), :][:, [0, 1]]

                # add (orderId, userId) to suspicious_orders
                for order in bad_orders:
                    suspicious_orders.add(tuple(order))

                last_upper_time = upper_time

        return list(suspicious_orders)

    def get_suspicious_shop_users(self) -> dict:
        """
        retrieve shopId and their suspicious userId


        Returns
        -------
        set
            Set of suspicious shop and users in a dictionary {shopId : (userId1, userId2)}
            or {shopId : None} if no suspicious transaction detected.

        """
        shopList: dict = dict()

        for shopId in self.__UNIQUE_SHOP_ID:

            # get suspicious orders
            orders: np.ndarray = self.__get_shop_transaction__(shopId)
            bad_order: list = self.__get_suspicious_orders__(orders)

            # add None if no suspicious transactions
            if len(bad_order) == 0:
                shopList[shopId] = None
                continue

            suspicious = np.array(bad_order, dtype=np.int64)

            # count number of suspicious transactions for each user
            countUnique = np.unique(
                suspicious[:, 1].flatten(), return_counts=True)
            users: np.ndarray = countUnique[0]
            counts: np.ndarray = countUnique[1]

            # add users with max number of suspicious transactions
            maxCount: np.int64 = counts.max()
            shopList[shopId] = tuple(users[counts == maxCount].tolist())

        return shopList
