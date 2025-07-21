import psycopg2
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
conn = psycopg2.connect(
    host=os.getenv("DATABASE_SERVER_PG"),
    port=os.getenv("DATABASE_PORT_PG"),
    database=os.getenv("DATABASE_NAME_PG"),
    user=os.getenv("DATABASE_UID_PG"),
    password=os.getenv("DATABASE_PWD_PG"),
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
    ktp = generate_ktp()


    cursor.execute("""
        INSERT INTO users (
            id, full_name, email, phone, ktp, created_at
        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    """, (
        id_, full_name, email, phone, ktp
    ))

# Commit and close
conn.commit()
cursor.close()
conn.close()

end_time=time.time()
elapsed_time = end_time-start_time

print(f"Inserted 10,000 dummy rows successfully in {elapsed_time:.2f} seconds.")
