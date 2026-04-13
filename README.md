# ✈️ Airline ETL Pipeline

End-to-end ETL pipeline built with PySpark implementing
Bronze-Silver-Gold lakehouse architecture with SCD1 logic.

---

##  🏗️ Architecture

Raw Data (Bronze) -> Cleaned Data (Silver) -> Final Data (Gold)

## 📁 Project Structure

modules/
├── utility.py    → shared helper functions
├── airline.py    → airline ETL operations
├── airport.py    → airport ETL operations
├── plane.py      → plane ETL operations
└── routes.py     → route ETL operations
main.py           → entry point

---

## ⚙️ Tech Stack

- Python
- PySpark
- Parquet
- SCD Type 1

 ---

## 🔄 Pipeline Flow

1. Read raw CSV data
2. Assign Column names if not present
3. Apply business rule
4. Seperate good and bad data
5. Add load_date
6. Write to silver layer
7. Apply SCD1 logic
8. Write to GOld layer

---

## 📊 Datasets

- Airline -> 6000+ records
- Airport -> 10000+ records
- Plane -> 500+ records
- Routes -> 60000+ records

---

## 🧠 Concepts Used

- PySpark DataFrames
- Window Functions
- SCD Type 1
- Good/Bad data seperation
- Bronze - Silver - Gold architecture
- OOP - Classes and Methods
