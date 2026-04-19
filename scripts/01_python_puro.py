import csv as csv
import os

# 1. reading the dataset
with open('../data/Amazon Sale Report.csv', mode='r', encoding='utf-8') as file:
  dictionary = list(csv.DictReader(file))

print(dictionary[0])

# 2. Calculate total sales
errors = 0

def clean_amount(value):
  global errors
  try:
    return float(value)
  except ValueError:
    errors += 1
    return 0.0


amount_list = [clean_amount(row['Amount']) for row in dictionary]
# Sum amount
total_sales = sum(amount_list)

print(errors, total_sales)

# 3. Top 5 best-selling products by SKU
sku_counts = {}

for row in dictionary:
    sku = row.get('SKU')
    if not sku:
        continue

    try:
      qty = int(row.get('Qty', 0))
    except ValueError:
      qty = 0

    sku_counts[sku] = sku_counts.get(sku, 0) + qty

top_5_skus = sorted(sku_counts.items(), key=lambda item: item[1], reverse=True)[:5]


print("Top 5 best-selling products:")
for sku, total in top_5_skus:
    print(f"SKU: {sku} - Quantity: {total}")

# 4. New file with the filtered orders
final_columns = ['Order ID', 'SKU', 'Amount', 'Qty']

with open('../output/filter_orders.csv', mode='w', encoding='utf-8', newline='') as output_file:
  writer = csv.DictWriter(output_file, fieldnames=final_columns, extrasaction='ignore')
  writer.writeheader()
  writer.writerows(dictionary)
print("Process completed")