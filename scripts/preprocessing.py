#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Data integration - ETL after re-design ER diagram """

import os
import logging

import polars as pl

DEBUG = True
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG, format="%(levelname)s - [%(module)s %(funcName)s] %(message)s"
)
LOGGER.setLevel(logging.DEBUG if DEBUG else logging.INFO)

CSV_NAMELIST = [
    "sale_order",
    "canceled_order",
    "order_item",
    "canceled_item",
    "waybill",
    "address",
    "customer_code",
    "inventory",
    "product_code",
    "color_code",
    "material_code",
    "size_code",
    "price",
]


def get_cleaned_dataframe(csv_folder_path):
    """
    Read csv and create dataframe

    Returns:
    - df (dict): dataframe
    """
    df = {}
    for file_name in CSV_NAMELIST:
        # read csv
        df[f"{file_name}"] = pl.read_csv(
            f"{csv_folder_path}\{file_name}.csv",
            has_header=True,
            infer_schema_length=10000,
            null_values=["COMPUTED_VALUE"],
        )
        # remove all null columns
        df[f"{file_name}"] = df[f"{file_name}"][
            [
                column.name
                for column in df[f"{file_name}"]
                if not (column.null_count() == df[f"{file_name}"].height)
            ]
        ]
        # remove all null rows
        df[f"{file_name}"] = df[f"{file_name}"].filter(
            ~pl.all_horizontal(pl.all().is_null())
        )

    LOGGER.info(df)
    return df


def rename_columns(df):
    """
    Rename columns

    Args:
    - df (dict): dataframe
    """
    # sale_order
    LOGGER.debug("Source sale_order: %s", df["sale_order"].columns)
    df["sale_order"] = df["sale_order"].drop(
        [
            "invoice no",
            "invoice folder",
            "invoice page",
            "note",
            "pdf",
            "shiping_status",
        ]
    )
    df["sale_order"] = df["sale_order"].rename(
        {
            "id": "order_id",
            "record date": "updated_at",
            "sell out date": "created_at",
            "invoice no2": "invoice_number",
            "order number": "order_number",
            "sales channel": "sales_channel",
            "require vat": "require_vat",
            "shipping charges": "shipping_charge",
            "discount bath": "discount_baht",
            "customer code": "customer_category_id",
            "customer name": "name",
            "customer tax id": "tax_id",
            "customer address": "billing_address",
            "shipping address": "shipping_address",
            "customer tel": "phone",
            "customer email": "email",
            "Status": "status",
        }
    )
    LOGGER.debug("Renamed sale_order's columns: %s", df["sale_order"].columns)


if __name__ == "__main__":
    current_dir = os.getcwd()
    dataframe_dict = get_cleaned_dataframe(
        f"{current_dir}\example_data\\renamed_source_database"
    )
    rename_columns(dataframe_dict)
