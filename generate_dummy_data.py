import pyodbc
import random
import uuid
from faker import Faker
import time

import os
from dotenv import load_dotenv
load_dotenv()

start_time = time.time()

fake = Faker('id_ID')

# DB connection
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={os.getenv("DATABASE_SERVER")};'
    f'DATABASE={os.getenv("DATABASE_NAME")};'
    f'UID={os.getenv("DATABASE_UID")};'
    f'PWD={os.getenv("DATABASE_PWD")}'
)
cursor = conn.cursor()

# Helper functions
def random_gender():
    return random.choice(['Pria', 'Wanita'])

def random_birthdate():
    return fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d')

def generate_ktp():
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def generate_npwp():
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

# Insert loop
for _ in range(10000):
    id_ = str(uuid.uuid4())
    full_name = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    gender = random_gender()
    ktp = generate_ktp()
    npwp = generate_npwp()
    address = fake.street_address()
    city = fake.city()
    province = fake.state()
    birth_day = random_birthdate()
    birth_place = fake.city()
    nationality = "Indonesia"

    cursor.execute("""
        INSERT INTO users_user (
            id, full_name, email, phone, gender, ktp, npwp, address,
            city, province, birth_day, birth_place, nationality, create_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """, (
        id_, full_name, email, phone, gender, ktp, npwp, address,
        city, province, birth_day, birth_place, nationality
    ))

# Commit and close
conn.commit()
cursor.close()
conn.close()

end_time=time.time()
elapsed_time = end_time-start_time

print(f"Inserted 10,000 dummy rows successfully in {elapsed_time:.2f} seconds.")
