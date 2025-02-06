import pandas as pd
import numpy as np
from datetime import datetime


num_records = 527040
start_time = datetime(2025, 1, 1, 0, 0)

timestamps = pd.date_range(start=start_time, periods=num_records, freq="min")
ids = [f"{i+1}" for i in range(num_records)]

def generate_stock_price(timestamps, min_price=1.0):
    """
    Generate stock prices for each timestamp.
    
    Args:
        timestamps (DatetimeIndex): A sequence of timestamps.
        min_price (float, optional): The minimum allowed price. Defaults to 1.0.
    
    Returns:
        list: A list of stock prices (rounded to two decimal places).
    """
    base_price = 100
    trend = 0.0002
    volatility = 2
    prices = []
    current_price = base_price
    for _ in range(len(timestamps)):
        current_price += np.random.normal(0, volatility) + trend
        current_price = max(current_price, min_price)
        prices.append(np.round(current_price, 2))
    return prices

def generate_sales(timestamps):
    """
    Generate sales trends based on seasonality and random noise.
    
    Args:
        timestamps (DatetimeIndex): A sequence of timestamps.
    
    Returns:
        list: A list of sales trend values (rounded to two decimal places).
    """
    base_sales = 200
    seasonality_amplitude = 50
    sales = []
    for timestamp in timestamps:
        day_of_year = timestamp.timetuple().tm_yday
        seasonal_effect = seasonality_amplitude * np.sin(2 * np.pi * (day_of_year / 365))
        noise = np.random.normal(0, 5)
        sales_value = base_sales + seasonal_effect + noise
        sales.append(np.round(sales_value, 2))
    return sales

# Generate synthetic values
stock_price = generate_stock_price(timestamps)
sales_trend = generate_sales(timestamps)

# Create a DataFrame with the generated data
data = {
    "id": ids,
    "timestamp": timestamps,
    "stock_price": stock_price,
    "sales_trend": sales_trend,
}
synthetic_data = pd.DataFrame(data)

# Save the DataFrame as a Parquet file
synthetic_data.to_parquet("synthetic_data.parquet", index=False)
print("Data saved as Parquet.")
