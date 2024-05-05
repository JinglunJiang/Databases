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

# Read and preprocess guest data from CSV file


def read_guest_data(connection):
    try:
        with open('./Data/hotel_guests.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Preprocess data and insert into Guests table
                guest_id = int(row['GuestID'])
                full_name = row['FullName']
                email = row['Email']
                age = int(row['Age'])
                state = row['State']

                # Remove titles like 'Mr.' or 'Dr.' if present
                full_name = full_name.replace("Mr. ", "").replace("Dr. ", "")

                # Split full name into first name and last name
                parts = full_name.split(maxsplit=1)  # Split into two parts
                if len(parts) == 2:
                    first_name, last_name = parts
                else:
                    # If full name has only one part, assume it's the first name
                    first_name = parts[0]
                    last_name = ""

                # Insert data into Guests table
                cursor = connection.cursor()
                insert_query = "INSERT INTO Guests (GuestID, FirstName, LastName, Email, Age, State) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, (guest_id, first_name,
                               last_name, email, age, state))
                connection.commit()
                cursor.close()
        print("Guest data inserted successfully.")
    except Exception as e:
        print("Error reading guest data:", e)


# Read and preprocess reservation data from CSV file
def read_reservation_data(connection):
    try:
        with open('./Data/hotel_reservations.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Preprocess data and insert into Reservations table
                reservation_id = row['ReservationID']
                guest_id = row['GuestID']
                room_number = row['RoomNumber']
                check_in_date = row['CheckInDate']
                num_nights = row['NumberOfNights']
                total_cost = row['TotalCost'].replace('$', '')

                # Insert data into Reservations table
                cursor = connection.cursor()
                insert_query = "INSERT INTO Reservations (ReservationID, GuestID, RoomNumber, CheckInDate, NumberOfNights, TotalCost) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, (reservation_id, guest_id,
                               room_number, check_in_date, num_nights, total_cost))
                connection.commit()
                cursor.close()

        print("Reservation data inserted successfully.")
    except Exception as e:
        print("Error reading reservation data:", e)

# Read and preprocess room data from CSV file


def read_room_data(connection):
    try:
        with open('./Data/hotel_rooms.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Preprocess data and insert into Rooms table
                room_number = row['RoomNumber']
                room_type = row['RoomType']
                floor_number = row['FloorNumber']

                # Insert data into Rooms table
                cursor = connection.cursor()
                insert_query = "INSERT INTO Rooms (RoomNumber, RoomType, FloorNumber) VALUES (%s, %s, %s)"
                cursor.execute(
                    insert_query, (room_number, room_type, floor_number))
                connection.commit()
                cursor.close()

        print("Room data inserted successfully.")
    except Exception as e:
        print("Error reading room data:", e)


# Query to display guests from specific states with ages between 25 and 30
def query_guests_by_state_and_age(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT CONCAT(FirstName, ' ', LastName) AS FullName, Email
        FROM Guests
        WHERE State IN ('Illinois', 'Indiana', 'Michigan')
        AND Age BETWEEN 25 AND 30
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("Guests from Illinois, Indiana, and Michigan with ages between 25 and 30:")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print("Error executing query:", e)

# Query to display guests who reserved rooms of type "Suite"


def query_guests_by_room_type(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT CONCAT(FirstName, ' ', LastName) AS FullName, State
        FROM Guests
        JOIN Reservations ON Guests.GuestID = Reservations.GuestID
        JOIN Rooms ON Reservations.RoomNumber = Rooms.RoomNumber
        WHERE Rooms.RoomType = 'Suite'
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("Guests who reserved rooms of type 'Suite':")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print("Error executing query:", e)

# Query to display available room types, number of reservations made, and average number of nights


def query_room_availability(connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT RoomType, COUNT(Reservations.RoomNumber) AS NumReservations, AVG(NumberOfNights) AS AvgNights
        FROM Rooms
        LEFT JOIN Reservations ON Rooms.RoomNumber = Reservations.RoomNumber
        GROUP BY RoomType
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print("Room availability:")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print("Error executing query:", e)

# Main function to execute the data import process


def main():
    # Connect to the MySQL database
    connection = connect_to_database()
    if connection is None:
        return

    # # Read and insert guest data
    # read_guest_data(connection)

    # # Read and insert reservation data
    # read_reservation_data(connection)

    # # Read and insert room data
    # read_room_data(connection)

    # Commit changes and close connection
    # connection.commit()

    # Execute queries
    query_guests_by_state_and_age(connection)
    query_guests_by_room_type(connection)
    query_room_availability(connection)

    connection.close()
    print("Data import process completed")


if __name__ == "__main__":
    main()
