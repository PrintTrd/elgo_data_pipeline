#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Data integration - ETL after re-design ER diagram """

import os
import glob
import logging

import polars as pl

DEBUG_MODE = False
LOGGER = logging.getLogger("scripts" + __name__)
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format="%(levelname)s - [%(module)s %(funcName)s]: %(message)s",
)

SOURCE_FOLDER = f"{os.getcwd()}\example_data\\renamed_source_database"
CSV_FILE_LIST = glob.glob(f"{SOURCE_FOLDER}\*.csv")
CSV_NAME_LIST = [
    os.path.splitext(os.path.basename(files))[0] for files in CSV_FILE_LIST
]


def get_dataframe():
    """
    Read csv, create dataframe and remove all null columns/rows.
    This function can apply to automate pipeline in future.

    Returns:
    - df (dict): dataframe
    """
    df = {}
    for file_name in CSV_NAME_LIST:
        # read csv
        df[f"{file_name}"] = pl.read_csv(
            f"{SOURCE_FOLDER}\{file_name}.csv",
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
        # replace "-" with null, otherwise keep original value
        df[f"{file_name}"] = df[f"{file_name}"].with_columns(
            pl.when(pl.col(pl.String) == "-")
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )
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

    return df


def fill_null_and_change_type_format(df):
    for df_name in CSV_NAME_LIST:
        for column in df[df_name]:
            if column.name in ["billing_address", "shipping_address"]:
                df[df_name] = df[df_name].with_columns(
                    pl.col("billing_address").fill_null(pl.col("shipping_address")),
                    pl.col("shipping_address").fill_null(pl.col("billing_address")),
                )
            if "_at" in column.name and column.dtype != pl.Datetime:
                df[df_name] = df[df_name].with_columns(
                    pl.coalesce(
                        pl.col(column.name).str.strptime(
                            pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False
                        ),
                        pl.col(column.name).str.strptime(
                            pl.Date, "%d/%m/%Y", strict=False
                        ),
                    )
                )
            if column.name == "phone":
                df[df_name] = df[df_name].with_columns(
                    pl.col("phone").cast(pl.String),
                )
                # replace "-" in phone number like 080-000-000
                df[df_name] = df[df_name].with_columns(
                    pl.when(pl.col("phone").str.contains_any(["-"]))
                    .then(pl.col("phone").str.replace_all("-", ""))
                    .when(pl.col("phone").str.contains("[6][6][0-9]{9}$|[0][0]"))
                    .then(pl.col("phone").str.replace("^([6][6]|[0][0])", "0"))
                    .when(pl.col("phone").str.contains("[6][6][0][0-9]{9}$"))
                    .then(pl.col("phone").str.replace("^[6][6]", ""))
                    .otherwise(pl.col("phone"))
                )
                phone_filter = (
                    df[df_name]
                    .select(pl.col(pl.String))
                    .filter(pl.col("phone").str.contains("^([6][6]|[0][0])"))
                )
                LOGGER.info(f"{df_name}: {phone_filter}")

    return df


# def clean_phone_number(number):
#     number = number.lstrip("+").lstrip("0").replace("-", "")
#     if not number.startswith("66"):
#         number = "66" + number
#     if len(number) != 13 or not number.isdigit():
#         number = "Invalid Number"

#     return number


def add_fake_data(df):
    from faker import Faker

    fake = Faker("th_TH")
    # lambda x: fake.name()


if __name__ == "__main__":
    dataframe_dict = get_dataframe()
    extracted_df = rename_columns(dataframe_dict)
    # remove "-CN" from invoice_number column and add "Canceled" to new status column
    extracted_df["canceled_order"] = extracted_df["canceled_order"].with_columns(
        pl.lit("Canceled").alias("status"),
        pl.col("invoice_number").str.replace("-CN", ""),
    )
    transformed_df = fill_null_and_change_type_format(extracted_df)

    # combine data and check rows
    LOGGER.debug(transformed_df["sale_order"].select(pl.count("order_id")))
    LOGGER.debug(transformed_df["canceled_order"].select(pl.count("cancel_id")))
    transformed_df["sale_order"] = pl.concat(
        [transformed_df["sale_order"], transformed_df["canceled_order"]],
        how="diagonal",
    )
    LOGGER.debug(transformed_df["sale_order"].select(pl.count("order_id")))

    transformed_df["sale_order"] = transformed_df["sale_order"].with_columns(
        pl.col("created_at").fill_null(pl.col("updated_at"))
    )
    LOGGER.debug("combined sale_order table: %s", transformed_df["sale_order"])
    LOGGER.debug(transformed_df["sale_order"].filter(pl.col("status") == "Canceled"))

    # add fake data
    # add_fake_data(transformed_df)
