
import polars as pl

dataframes = {}
csv_list = ["sale_order", "canceled_order", "order_item", "canceled_item", "waybill", "address", "customer_code", "inventory", "product_code", "color_code", "material_code", "size_code", "price"]

for file_name in csv_list:
  dataframes[f'{file_name}'] = pl.read_csv(f'..\example_data\from_source_database\{file_name}.csv', has_header=True, infer_schema_length=10000, null_values=["COMPUTED_VALUE"])
print(dataframes)

print(dataframes["sale_order"].columns)
sale_order_df = dataframes["sale_order"].drop(columns=['invoice no', 'invoice folder', 'invoice page', 'note', 'pdf', "shiping_status"])
sale_order_df = sale_order_df.rename({
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
    "Status":"status"
})
print(sale_order_df.columns)
