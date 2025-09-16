import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import traceback

# --- Page Configuration ---
st.set_page_config(layout="wide")

# --- ==================================================================== ---
# ---     PART 1: DATABASE LOGIC (Modified for Testing)
# --- ==================================================================== ---

@st.cache_resource
def get_engine():
    """Creates a cached database engine"""
    try:
        DB_HOST = st.secrets["database"]["host"]
        DB_PORT = st.secrets["database"]["port"]
        DB_NAME = st.secrets["database"]["dbname"]
        DB_USER = st.secrets["database"]["user"]
        DB_PASS = st.secrets["database"]["password"]
        return create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
            poolclass=NullPool
        )
    except Exception as e:
        st.error("Database connection failed. Please check your [database] credentials in secrets.toml.")
        st.code(traceback.format_exc())
        return None

engine = get_engine()
if engine is None:
    st.stop()

@st.cache_data(ttl=10) # Short cache for testing
def load_data():
    """Loads data from the database"""
    try:
        with engine.connect() as connection:
            query = """
                SELECT 
                    d.equipment_tag_id, d.equipment_name, d.component, d.point_measurement,
                    d.date, d.value, d.unit, d.status, d.note, d.alarm_standard,
                    stds.excellent, stds.acceptable, stds.requires_evaluation, stds.unacceptable
                FROM data d
                LEFT JOIN alarm_standards stds ON d.alarm_standard = stds.standard
                WHERE d.value IS NOT NULL
                LIMIT 1000  -- <-- ADDED LIMIT FOR TESTING
            """
            df = pd.read_sql(query, connection)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True)
        return df
    except Exception as e:
        st.error("Failed to load data from the database. This is likely a firewall or network issue.")
        st.code(traceback.format_exc())
        return pd.DataFrame()
        
# --- ==================================================================== ---
# ---     PART 2: SIMPLE APP TO DISPLAY DATA
# --- ==================================================================== ---

st.title("Database Connection and Data Load Test 🧪")

st.info("""
This app tests the database connection.
- If it shows a **'Database connection failed'** error, your secrets are wrong.
- If it **loads forever**, your database firewall is blocking Streamlit.
- If it **shows a table**, your connection is good!
""")

if st.button("🔄 Rerun Test (Clear Cache)"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

with st.spinner("Attempting to connect and load data..."):
    df = load_data()

if not df.empty:
    st.success(f"✅ Success! Connected and loaded {len(df)} rows.")
    st.dataframe(df)
else:
    # The error (if any) is already displayed by load_data()
    st.warning("⚠️ Connection test ran, but the query returned no data (or failed).")
