import mysql.connector
import csv

# Connect to the MySQL database


def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="9613192796Jjl_",
            database="MyHotel"
        )
        print("Connected to MySQL database")
        return connection
    except mysql.connector.Error as error:
        print("Failed to connect to MySQL database:", error)
        return None


# Read and preprocess product data from CSV file
def read_product_data(connection):
    try:
        with open('./Data/ecommerce_products.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Preprocess data and insert into Products table
                pid = row['PID']
                name = row['Name']
                vendor = row['Vendor']
                available_quantity = int(row['AvailbleQuantity'])

                # Insert data into Products table
                cursor = connection.cursor()
                insert_query = "INSERT INTO Products (PID, Name, Vendor, AvailableQuantity) VALUES (%s, %s, %s, %s)"
                cursor.execute(
                    insert_query, (pid, name, vendor, available_quantity))
                connection.commit()
                cursor.close()
        print("Product data inserted successfully.")
    except Exception as e:
        print("Error reading product data:", e)


# Read and preprocess customer data from CSV file
def read_customer_data(connection):
    try:
        with open('./Data/ecommerce_customers.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Preprocess data and insert into Customers table
                cid = row['CID']
                full_name = row['FullName']
                email = row['Email']
                age = int(row['Age'])
                # Update to match the CSV column name
                address = row['StreetAddress']
                state = row['State']

                # Insert data into Customers table
                cursor = connection.cursor()
                insert_query = "INSERT INTO Customers (CID, FullName, Email, Age, Address, State) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(
                    insert_query, (cid, full_name, email, age, address, state))
                connection.commit()
                cursor.close()
        print("Customer data inserted successfully.")
    except Exception as e:
        print("Error reading customer data:", e)

# Read and preprocess order data from CSV file


def read_order_data(connection):
    try:
        with open('./Data/ecommerce_orders.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Preprocess data and insert into Orders table
                oid = int(row['OID'])
                pid = int(row['PID'])
                cid = int(row['CID'])
                order_quantity = int(row['OrderQuantity'])
                order_date = row['OrderDate']
                total_cost = int(row['TotalCost'])

                # Insert data into Orders table
                cursor = connection.cursor()
                insert_query = "INSERT INTO Orders (OID, PID, CID, OrderQuantity, OrderDate, TotalCost) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(
                    insert_query, (oid, pid, cid, order_quantity, order_date, total_cost))
                connection.commit()
                cursor.close()
        print("Order data inserted successfully.")
    except Exception as e:
        print("Error reading order data:", e)


# Query to display product vendors with more than 5 unique products and total quantity available
def query_product_vendors(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT Vendor, COUNT(DISTINCT PID) AS UniqueProducts, SUM(AvailableQuantity) AS TotalQuantity
        FROM Products
        GROUP BY Vendor
        HAVING UniqueProducts > 5
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("Product vendors with more than 5 unique products:")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print("Error executing query:", e)

# Query to display customers who made orders with total cost greater than 1000$


def query_customers_by_order_cost(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT CONCAT(FullName, ', ', State, ', ', Age) AS CustomerDetails
        FROM Customers
        WHERE CID IN (
            SELECT DISTINCT CID
            FROM Orders
            GROUP BY CID
            HAVING SUM(TotalCost) > 1000
        )
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("Customers who made orders with total cost greater than 1000$:")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print("Error executing query:", e)

# Query to display orders made in 2023 with total cost greater than the average of all orders


def query_orders_in_2023(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT *
        FROM Orders
        WHERE YEAR(OrderDate) = 2023
        AND TotalCost > (
            SELECT AVG(TotalCost)
            FROM Orders
        )
        ORDER BY TotalCost DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("Orders made in 2023 with total cost greater than average of all orders:")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print("Error executing query:", e)


def main():
    # Connect to the MySQL database
    connection = connect_to_database()
    if connection is None:
        return

    # Read and insert customers data
    read_customer_data(connection)

    # Read and insert products data
    read_product_data(connection)

    # Read and insert orders data
    read_order_data(connection)

    query_product_vendors(connection)
    query_customers_by_order_cost(connection)
    query_orders_in_2023(connection)

    connection.close()
    print("Data import process completed")


if __name__ == "__main__":
    main()
