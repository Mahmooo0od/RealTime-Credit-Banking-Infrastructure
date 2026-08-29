import pandas as pd
import psycopg2
import time
import os

# بيانات الاتصال بقاعدة البيانات
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "home_credit_db")
DB_USER = os.getenv("POSTGRES_USER", "iscore_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "iscore_password")

# مسار ملف الـ CSV
CSV_FILE_PATH = "data/raw_source/application_train.csv"

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def stream_applications():
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: File {CSV_FILE_PATH} not found!")
        return

    print("Reading CSV dataset...")
    # قراءة البيانات كـ Chunks لتوفير الذاكرة
    chunksize = 100
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO banking_core.applications (
        sk_id_curr, target, name_contract_type, code_gender, flag_own_car,
        flag_own_realty, cnt_children, amt_income_total, amt_credit,
        amt_annuity, amt_goods_price, name_income_type, name_education_type,
        name_family_status, name_housing_type, days_birth, days_employed, occupation_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (sk_id_curr) DO NOTHING;
    """

    print("Starting streaming applications to PostgreSQL...")
    
    for chunk in pd.read_csv(CSV_FILE_PATH, chunksize=chunksize):
        for _, row in chunk.iterrows():
            # استبدال قيم NaN بـ None متوافقة مع SQL NULL
            data = tuple(None if pd.isna(val) else val for val in [
                row['SK_ID_CURR'], row.get('TARGET'), row.get('NAME_CONTRACT_TYPE'), row.get('CODE_GENDER'),
                row.get('FLAG_OWN_CAR'), row.get('FLAG_OWN_REALTY'), row.get('CNT_CHILDREN'), row.get('AMT_INCOME_TOTAL'),
                row.get('AMT_CREDIT'), row.get('AMT_ANNUITY'), row.get('AMT_GOODS_PRICE'), row.get('NAME_INCOME_TYPE'),
                row.get('NAME_EDUCATION_TYPE'), row.get('NAME_FAMILY_STATUS'), row.get('NAME_HOUSING_TYPE'),
                row.get('DAYS_BIRTH'), row.get('DAYS_EMPLOYED'), row.get('OCCUPATION_TYPE')
            ])
            
            cursor.execute(insert_query, data)
            conn.commit()
            
            print(f"Streamed Application ID: {row['SK_ID_CURR']}")
            time.sleep(0.5) # تأخير لمدة نصف ثانية لمحاكاة التدفق الحقيقي

    cursor.close()
    conn.close()

if __name__ == "__main__":
    stream_applications()
