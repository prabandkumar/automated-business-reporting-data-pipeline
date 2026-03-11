import pandas as pd

print("Pipeline started")

def run_pipeline():

    orders = pd.read_csv(r"C:\Users\praba\OneDrive\Desktop\automated-business-reporting-data-pipeline\data\raw\orders.csv")

    orders["order_date"] = pd.to_datetime(orders["order_date"])

    orders["year"] = orders["order_date"].dt.year
    orders["month"] = orders["order_date"].dt.month
    orders["week"] = orders["order_date"].dt.isocalendar().week

    total_revenue = orders["order_value"].sum()
    total_orders = orders["order_id"].count()
    avg_order_value = orders["order_value"].mean()

    report = pd.DataFrame({
        "Metric": ["Total Revenue", "Total Orders", "Average Order Value"],
        "Value": [total_revenue, total_orders, avg_order_value]
    })

    report.to_csv(r"C:\Users\praba\OneDrive\Desktop\automated-business-reporting-data-pipeline\reports\business_report.csv", index=False)

    print("Report generated successfully")


run_pipeline()