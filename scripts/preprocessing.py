#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Data integration - ETL after re-design ER diagram """

import os
import glob
import logging

import polars as pl

DEBUG = False
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s - [%(module)s %(funcName)s()] %(message)s",
)
LOGGER.setLevel(logging.DEBUG if DEBUG else logging.INFO)


def get_dataframe(csv_folder_path):
    """
    Read csv, create dataframe and remove all null columns/rows.
    This function can apply to automate pipeline in future.

    Returns:
    - df (dict): dataframe
    """
    df = {}
    csv_file_list = glob.glob(f"{csv_folder_path}\*.csv")
    for files in csv_file_list:
        file_name = os.path.splitext(os.path.basename(files))[0]
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

    LOGGER.debug(df)
    return df


def rename_columns(df):
    """
    Rename columns

    Args:
    - df (dict): dataframe

    Returns:
    - df (dict): processed dataframe
    """
    # sale_order
    if not df["sale_order"].is_empty():
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

    # canceled_order
    if not df["canceled_order"].is_empty():
        LOGGER.debug("Source canceled_order: %s", df["canceled_order"].columns)
        df["canceled_order"] = df["canceled_order"].drop(["image", "issue date"])
        df["canceled_order"] = df["canceled_order"].rename(
            {
                "id": "cancel_id",
                "creditnote no": "updated_at",
                "invoice id": "invoice_number",
                "order number": "order_number",
                "sales channel": "sales_channel",
                "require vat": "require_vat",
                "shipping charges": "shipping_charge",
                "discount bath": "discount_baht",
                "customer name": "name",
                "customer address": "billing_address",
                "shipping address": "shipping_address",
                "customer tel": "phone",
                "note": "cancel_reason",
            }
        )
        LOGGER.debug(
            "Renamed canceled_order's columns: %s", df["canceled_order"].columns
        )

    # order_item
    if not df["order_item"].is_empty():
        LOGGER.debug("Source order_item: %s", df["order_item"].columns)
        df["order_item"] = df["order_item"].drop(
            [
                "master product name",
                "readable name",
                "product code",
                "color code",
                "size code",
                "pcs per pack",
                "quantity",
            ]
        )
        df["order_item"] = df["order_item"].rename(
            {
                "id": "order_item_id",
                "sell out date": "created_at",
                "order id": "order_id",
                "master product code": "master_product_code",
                "price per pack": "unit_price",
                "price per pack (ex vat)": "unit_price_ex_vat",
                "quantity (pack)": "quantity_pack",
                "total amount": "total_price",
            }
        )
        LOGGER.debug("Renamed order_item's columns: %s", df["order_item"].columns)

    # canceled_item
    if not df["canceled_item"].is_empty():
        LOGGER.debug("Source canceled_item: %s", df["canceled_item"].columns)
        df["canceled_item"] = df["canceled_item"].drop(
            [
                "readable name",
                "product code",
                "color code",
                "size code",
                "color code",
                "pcs per pack",
            ]
        )
        df["canceled_item"] = df["canceled_item"].rename(
            {
                "id": "order_item_id",
                "cn id": "order_id",
                "master product code": "master_product_code",
                "price per pack": "unit_price",
                "quantity (pack)": "quantity_pack",
                "total amount": "total_price",
            }
        )
        LOGGER.debug("Renamed canceled_item's columns: %s", df["canceled_item"].columns)

    # waybill
    if not df["waybill"].is_empty():
        LOGGER.debug("Source waybill: %s", df["waybill"].columns)
        df["waybill"] = df["waybill"].drop(
            ["file", "platform", "price", "sales_channel_id"]
        )
        df["waybill"] = df["waybill"].rename(
            {"ordernumber": "order_number", "address": "shipping_address"}
        )
        LOGGER.debug("Renamed waybill's columns: %s", df["waybill"].columns)

    # customer_code
    if not df["customer_code"].is_empty():
        LOGGER.debug("Source customer_code: %s", df["customer_code"].columns)
        df["customer_code"] = df["customer_code"].drop(
            ["category", "number", "contact person", "require tax"]
        )
        df["customer_code"] = df["customer_code"].rename(
            {
                "code": "customer_category_id",
                "customer address": "shipping_address",
                "billing address": "billing_address",
                "tax id": "tax_id",
                "tel": "phone",
                "description": "note",
                "channel catagory": "channel_catagory",
            }
        )
        LOGGER.debug("Renamed customer_code's columns: %s", df["customer_code"].columns)

    LOGGER.info(df)
    return df


if __name__ == "__main__":
    current_dir = os.getcwd()
    dataframe_dict = get_dataframe(
        f"{current_dir}\example_data\\renamed_source_database"
    )
    processed_df = rename_columns(dataframe_dict)
