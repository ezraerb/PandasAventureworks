#!/usr/bin/python
""" 
Perform various analyses on the AdventureWorks data set using pandas. This data set is one of 
the classic sets for demonstrating ETL and analysis code. It is publicly available at 
https://github.com/lorint/AdventureWorks-for-Postgres
"""

from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.sql import text


def get_config(filename: str = "database.ini") -> dict:
    """Read access credentials for Postgres"""
    config = "postgresql"

    parser = ConfigParser()
    parser.read(filename)

    db_config = {}
    if parser.has_section(config):
        params = parser.items(config)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise ValueError(f"Section postgresql not found in {filename}")
    return db_config


def connect():
    """Connect to the PostgreSQL database server"""
    try:
        params = get_config()
        # Pandas recommends using a SQLAlchemy engine for the connrection,
        # requiring the following
        url = URL.create(drivername="postgresql", **params)
        return create_engine(url)

    except Exception as error:  # pylint: disable=broad-exception-caught
        print(error)
        return None


def inventory_by_product_model(engine):
    """Calculate the total inventory by product model"""

    with engine.connect() as conn:
        sql = text("select productid, productmodelid from production.product")
        product = pd.read_sql(sql, conn)
        sql = text("select productid, quantity from production.productinventory")
        inventory = pd.read_sql(sql, conn)

        inventory = inventory.merge(product, on="productid")
        totals = inventory.groupby("productmodelid", dropna=False)[["quantity"]].sum()
        totals.to_csv("inventory_by_product_model.csv")


def cost_per_location(engine):
    """
    Calculate the total amount it cost to make the product currenty at each
    location
    """
    with engine.connect() as conn:
        sql = text("select productid, standardcost from production.product")
        product = pd.read_sql(sql, conn)
        sql = text(
            "select productid, locationid, quantity from production.productinventory"
        )
        inventory = pd.read_sql(sql, conn)
        inventory = inventory.merge(product, on="productid")
        inventory["totalcost"] = inventory["quantity"] * inventory["standardcost"]
        totals = inventory.groupby("locationid", dropna=False)[["totalcost"]].sum()
        totals.to_csv("total_cost_by_location.csv")


def discount_per_product_id(engine):
    """
    Calculate the total discount for sales transactions for different products in differet
    quarters, for single vs. multiple quantity
    """
    with engine.connect() as conn:
        sql = text(
            "select productid, transactiondate, quantity, actualcost from "
            "production.transactionhistory "
            "where transactiontype = 'S' order by productid, transactiondate"
        )
        transactions = pd.read_sql(sql, conn)

        # Flag the multi-item transactions
        transactions["multiitem"] = transactions["quantity"] > 1

        # Need to index for the join
        transactions = transactions.set_index(
            ["productid", "transactiondate", "multiitem"]
        )

        sql = text(
            "select productid, startdate, listprice from production.productlistpricehistory "
            "order by productid, startdate"
        )
        price_history = pd.read_sql(sql, conn)
        price_history = price_history.set_index(["productid", "startdate"])

        # Need the price hisory data to contain a value for every combination of product id and
        # transaction date in the transactions list. This can be done by a reindex. Merge the
        # transaction dates to the existing rate dates to get the wanted index. This only works
        # because a transaction can only be generated when a product price exists. Note the name
        # change of the date index
        new_date_index = (
            price_history.index.get_level_values("startdate")
            .union(transactions.index.get_level_values("transactiondate"))
            .unique()
        )
        price_history = price_history.reindex(
            pd.MultiIndex.from_product(
                [
                    price_history.index.get_level_values("productid").unique(),
                    new_date_index,
                ],
                names=["productid", "transactiondate"],
            )
        ).fillna(method="ffill")

        transactions = transactions.join(price_history)

        # actualcost is per unit.
        transactions["totaldiscount"] = (
            transactions["listprice"] - transactions["actualcost"]
        ) * transactions["quantity"]
        discount_per_quarter = (
            transactions[["totaldiscount"]]
            .groupby(
                [
                    pd.Grouper(level="productid"),
                    pd.Grouper(level="multiitem"),
                    pd.Grouper(level="transactiondate", freq="Q"),
                ]
            )
            .sum()
        )
        discount_per_quarter.to_csv("total_prouct_discount_by_quarter.csv")


if __name__ == "__main__":
    db_engine = connect()
    if db_engine:
        inventory_by_product_model(db_engine)
        cost_per_location(db_engine)
        discount_per_product_id(db_engine)
