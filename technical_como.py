import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import traceback
import plotly.express as px
import requests

# --- Page Configuration ---
st.set_page_config(layout="wide")

# --- ==================================================================== ---
# ---    PART 1: FUNCTIONS (Logo, Database, Data Loading)
# --- ==================================================================== ---

@st.cache_data
def load_logo_from_repo():
    """Fetches the logo from a private GitHub repo, failing silently on any error."""
    OWNER_REPO = "AlvinWinarta2111/technical_condition_monitoring"
    LOGO_PATH = "images/alamtri_logo.jpeg"
    
    try:
        GITHUB_TOKEN = st.secrets["GITHUB_PRIVATE_TOKEN"]
    except KeyError:
        return None

    API_URL = f"https://api.github.com/repos/{OWNER_REPO}/contents/{LOGO_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw"
    }
    
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException:
        return None

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
        st.error("Error: Could Not Connect to Database", icon="🔥")
        st.warning("The application could not establish a connection to the database. This is likely due to incorrect credentials or a network issue.")
        st.info("Please contact the application administrator to verify the database connection settings (host, port, user, etc.) are correct in the deployment secrets.")
        return None

@st.cache_data(ttl=300)
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
                ORDER BY d.date DESC
                LIMIT 1000
            """
            df = pd.read_sql(query, connection)
            
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True)
        return df
    except Exception as e:
        st.error("Error: Failed to Load Monitoring Data", icon="📊")
        st.warning("The application connected to the database but failed to retrieve the monitoring data.")
        st.info("This can happen if the database tables (e.g., `data`, `alarm_standards`) have been changed or are missing. Please contact the administrator.")
        return pd.DataFrame()

# --- NEW HELPER FUNCTION FOR COLUMN MAPPING ---
def map_and_clean_columns(df):
    """
    Takes a DataFrame, normalizes its column names, and maps them to the standard database format.
    Also adds a new 'identifier' column if it doesn't exist.
    """
    
    # This dictionary maps various possible input names to the final database column names.
    COLUMN_MAPPING = {
        'identifier': 'identifier',
        'equipment_tag_id': 'equipment_tag_id',
        'equipment_name': 'equipment_name',
        'technology': 'technology',
        'component': 'component',
        'key': 'key',
        'alarm_standard': 'alarm_standard',
        'date': 'date',
        'measurement_point': 'point_measurement', # Note the mapping to the correct DB name
        'value': 'value',
        'unit': 'unit',
        'status': 'status',
        'excellent': 'excellent',
        'acceptable': 'acceptable',
        'alarm_yellow_warning': 'alarm_yellow_warning',
        'unacceptable_alarm': 'unacceptable',
        'note': 'note',
    }
    
    rename_dict = {}
    ignored_columns = []
    
    for col in df.columns:
        # Normalize: lowercase, strip whitespace, replace spaces and special chars with underscores
        normalized_col = str(col).lower().strip().replace(' ', '_').replace('(', '').replace(')', '')
        
        if normalized_col in COLUMN_MAPPING:
            rename_dict[col] = COLUMN_MAPPING[normalized_col]
        else:
            ignored_columns.append(col)
            
    # Add 'identifier' column with unique values if it wasn't in the original file
    # We will base it on the index to ensure it's unique for this upload batch
    if 'identifier' not in rename_dict.values():
        df['identifier'] = [f"generated_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}_{i}" for i in range(len(df))]
        st.info("Note: 'identifier' column was not found and has been auto-generated.", icon="🤖")

    df.rename(columns=rename_dict, inplace=True)
    
    if ignored_columns:
        st.warning(f"The following columns were found in the file but will be ignored: {', '.join(ignored_columns)}", icon="⚠️")
        
    return df

# --- ==================================================================== ---
# ---    PART 2: APP INITIALIZATION & MAIN LOGIC
# --- ==================================================================== ---

logo_bytes = load_logo_from_repo()
engine = get_engine()
if engine is None:
    st.error("Stopping application because a database connection could not be established.")
    st.stop()

# --- Sidebar for Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Choose a page", ["Monitoring Dashboard", "Upload New Data", "Database Viewer"])

# --- PAGE 1: DASHBOARD ---
if page == "Monitoring Dashboard":
    # (This page's logic remains the same)
    logo_col, title_col = st.columns([1, 8])
    with logo_col:
        if logo_bytes:
            st.image(logo_bytes, width=150)
    with title_col:
        st.title("Technical Condition Monitoring Dashboard")
    
    df = load_data()
    
    if df.empty:
        st.warning("⚠️ No data available to display. (Or the data failed to load from the database)")
        st.stop()

    # ... (Rest of dashboard code is unchanged) ...
    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)
    with col1:
        equipment_options = sorted(df["equipment_name"].dropna().unique())
        equipment_choice = st.selectbox("Equipment", options=equipment_options)
    
    filtered_by_eq = df[df["equipment_name"] == equipment_choice]
    
    with col2:
        component_options = sorted(filtered_by_eq["component"].dropna().unique())
        component_choice = st.selectbox("Component", options=component_options)
    
    component_df = filtered_by_eq[filtered_by_eq["component"] == component_choice]
    
    with col3:
        point_options = sorted(component_df["point_measurement"].dropna().unique())
        point_choices = st.multiselect("Measurement Point(s)", options=point_options)

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    if point_choices:
        filtered_df = component_df[component_df["point_measurement"].isin(point_choices)].copy()
        st.header(f"Results for: {equipment_choice} → {component_choice}")
        
        professional_color_palette = [
            '#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A',
            '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'
        ]
        unique_points = sorted(filtered_df['point_measurement'].unique())
        color_map = {point: professional_color_palette[i % len(professional_color_palette)] for i, point in enumerate(unique_points)}

        plot_df = filtered_df.sort_values(by="date")
        fig = px.line(
            plot_df, x="date", y="value", color="point_measurement", markers=True,
            title="Selected Measurement Points Trend (Last 1000 Records)",
            color_discrete_map=color_map
        )
        fig.update_layout(legend_title="Measurement Point", hovermode="x unified")
        
        notes_df = plot_df.dropna(subset=['note'])
        notes_df = notes_df[~notes_df['note'].astype(str).str.strip().isin(['', '-'])]
        
        for index, row in notes_df.iterrows():
            point_name = str(row['point_measurement']).strip()
            line_color = color_map.get(point_name)
            solid_color, transparent_color = ('rgb(200, 200, 200)', 'rgba(200, 200, 200, 0.5)')
            if line_color:
                r, g, b = tuple(int(line_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
                solid_color = f'rgb({r}, {g}, {b})'
                transparent_color = f'rgba({r}, {g}, {b}, 0.5)'
            fig.add_shape(
                type="line", x0=row['date'], y0=0, x1=row['date'], y1=1, yref='paper',
                line=dict(color=transparent_color, width=1, dash="dot")
            )
            fig.add_annotation(
                x=row['date'], y=1.05, yref='paper', text=f"<b>{row['note']}</b><br>({row['point_measurement']})",
                showarrow=False, font=dict(size=10, color=solid_color), xanchor="center", align="center"
            )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Alarm Standards")
        alarm_cols = ["point_measurement", "equipment_tag_id", "alarm_standard", "excellent", "acceptable", "requires_evaluation", "unacceptable", "unit"]
        alarm_df = filtered_df[alarm_cols].drop_duplicates().reset_index(drop=True)
        alarm_df.index = alarm_df.index + 1
        st.dataframe(
            alarm_df, use_container_width=True, hide_index=False,
            column_config={
                "point_measurement": st.column_config.TextColumn(width="medium"),
                "equipment_tag_id": st.column_config.TextColumn(width="medium"),
                "alarm_standard": st.column_config.TextColumn(width="medium"),
                "excellent": st.column_config.TextColumn(width="small"),
                "acceptable": st.column_config.TextColumn(width="small"),
                "requires_evaluation": st.column_config.TextColumn(width="small"),
                "unacceptable": st.column_config.TextColumn(width="small"),
                "unit": st.column_config.TextColumn(width="small"),
            }
        )
        
        def color_status(val):
            val_lower = str(val).lower()
            if "excellent" in val_lower: return "background-color: rgba(0, 128, 0, 0.7); color: white;"
            elif "acceptable" in val_lower: return "background-color: rgba(50, 205, 50, 0.7); color: black;"
            elif "requires evaluation" in val_lower: return "background-color: rgba(255, 165, 0, 0.7); color: black;"
            elif "unacceptable" in val_lower: return "background-color: rgba(255, 0, 0, 0.7); color: white;"
            return ""

        st.header("Detailed Historical Data")
        for point in sorted(point_choices):
            st.subheader(f"History for: {point}")
            point_df = filtered_df[filtered_df['point_measurement'] == point].copy()
            
            if not point_df.empty:
                hist_cols = ["date", "value", "unit", "status", "note"]
                historical_df = point_df[hist_cols].sort_values(by="date", ascending=False).reset_index(drop=True)
                historical_df.index = historical_df.index + 1
                
                if not historical_df.empty:
                    historical_df['date'] = historical_df['date'].dt.strftime('%Y-%m-%d %H:%M')
                    st.dataframe(
                        historical_df.style.format({'value': '{:g}'}).applymap(color_status, subset=['status']),
                        use_container_width=True, hide_index=False,
                        column_config={
                            "date": st.column_config.TextColumn(width="small"),
                            "value": st.column_config.TextColumn(width="small"),
                            "unit": st.column_config.TextColumn(width="small"),
                            "status": st.column_config.TextColumn(width="medium"),
                            "note": st.column_config.TextColumn(width="large"),
                        }
                    )
                else: 
                    st.info("No data available for this point.")
            else: 
                st.info("No historical data to display for this point.")
            st.markdown("---")
    else:
        st.info("ℹ️ Please select one or more measurement points from the filters above to see the data.")

# --- PAGE 2: UPLOAD DATA ---
elif page == "Upload New Data":
    logo_col, title_col = st.columns([1, 8])
    with logo_col:
        if logo_bytes:
            st.image(logo_bytes, width=150)
    with title_col:
        st.title("Upload New Data")

    st.write("This uploader automatically detects column names and formats.")

    table_options = ["data", "alarm_standards", "equipment", "alarm", "component"]
    target_table = st.selectbox("1. Select table to add data to", options=table_options)
    uploaded_file = st.file_uploader("2. Choose a file", type=["csv", "xlsx"])

    if st.button("3. Upload and Add Data"):
        if uploaded_file is not None and target_table is not None:
            try:
                # --- Step 1: Read the raw file ---
                if uploaded_file.name.endswith('.csv'):
                    uploaded_file.seek(0)
                    first_line = uploaded_file.readline().decode('utf-8')
                    uploaded_file.seek(0)
                    delimiter = ';' if ';' in first_line else ','
                    st.info(f"Detected '{delimiter}' as the delimiter.")
                    upload_df = pd.read_csv(uploaded_file, sep=delimiter, encoding='utf-8-sig')
                elif uploaded_file.name.endswith('.xlsx'):
                    upload_df = pd.read_excel(uploaded_file, engine='openpyxl')
                else:
                    upload_df = None

                if upload_df is None:
                    st.error("Error: Could not read the uploaded file.", icon="📄")
                    st.stop()
                
                # --- Step 2: Map columns to database format ---
                st.info("Attempting to map file columns to database format...")
                upload_df = map_and_clean_columns(upload_df)
                
                st.write("Preview of data after column mapping:"); st.dataframe(upload_df.head())

                # --- Step 3: Standard data cleaning and validation ---
                if 'identifier' in upload_df.columns:
                    # Drop rows that have a completely blank identifier from the start
                    upload_df.dropna(subset=['identifier'], inplace=True)
                else:
                    st.error("Upload Failed: Critical 'identifier' column is missing after mapping.", icon="❌")
                    st.stop()
                
                if upload_df.empty:
                    st.error("Upload Failed: No valid data found after initial cleaning.", icon="❌")
                    st.stop()
                
                # --- Step 4: Check if all required DB columns are present ---
                st.info(f"Verifying required columns for the '{target_table}' table...")
                with engine.connect() as connection:
                    db_cols = pd.read_sql(text(f"SELECT * FROM {target_table} LIMIT 0"), connection).columns.tolist()
                
                upload_cols = upload_df.columns.tolist()
                missing_cols = set(db_cols) - set(upload_cols)

                if missing_cols:
                    st.error("Upload Failed: Missing Required Columns", icon="❌")
                    st.warning(f"Even after mapping, your file is missing the following required columns for the '{target_table}' table.")
                    st.json(sorted(list(missing_cols)))
                    st.stop()
                    
                # --- Step 5: Filter DataFrame to only include columns that exist in the DB table ---
                # This prevents errors if the file has extra, un-ignored columns.
                final_upload_df = upload_df[db_cols]
                
                # --- Step 6: Check for duplicates (using the cleaned data) ---
                unique_key_map = {'data': 'identifier', 'alarm_standards': 'standard', 'component': 'point'}
                unique_key = unique_key_map.get(target_table)
                if unique_key and unique_key in final_upload_df.columns:
                    
                    st.info(f"Validating data types in the '{unique_key}' column...")
                    final_upload_df[unique_key] = pd.to_numeric(final_upload_df[unique_key], errors='coerce')
                    final_upload_df.dropna(subset=[unique_key], inplace=True)

                    st.info(f"Checking for duplicate '{unique_key}' values in the database...")
                    upload_ids = final_upload_df[unique_key].astype(int).tolist()
                    
                    if upload_ids:
                        with engine.connect() as connection:
                            query = text(f'SELECT "{unique_key}" FROM "{target_table}" WHERE "{unique_key}" IN :ids')
                            existing_ids_df = pd.read_sql(query, connection, params={'ids': tuple(upload_ids)})
                        existing_ids = set(existing_ids_df[unique_key])
                        duplicate_ids = [id for id in upload_ids if id in existing_ids]
                        if duplicate_ids:
                            st.error(f"Upload Failed: {len(duplicate_ids)} Duplicate Entries Found", icon="❌")
                            st.warning(f"Your file contains entries where the '{unique_key}' already exists in the database. Each '{unique_key}' must be unique.")
                            st.info("Please remove or change the following duplicate entries from your file:")
                            st.json(sorted(list(set(duplicate_ids))))
                            st.stop()

                # --- Step 7: Append to database ---
                st.info(f"All checks passed. Appending {len(final_upload_df)} valid rows to '{target_table}'...")
                with engine.connect() as connection:
                    final_upload_df.to_sql(target_table, con=connection, if_exists='append', index=False)
                st.success(f"Successfully added {len(final_upload_df)} rows to the '{target_table}' table!", icon="🎉")
                st.info("Clearing data cache... The dashboard will show the new data on its next load.")
                st.cache_data.clear()

            except Exception as upload_error:
                st.error("An Unexpected Error Occurred During Upload", icon="🔥")
                st.warning("This could be due to issues like incorrect data types in columns (e.g., text in a number-only field) or other hidden formatting problems in your file.")
                st.info("Please review your file carefully. If the issue persists, show the details below to the app administrator.")
                with st.expander("Show Error Details for Administrator"):
                    st.code(traceback.format_exc())
        else:
            st.warning("⚠️ Please select a table and upload a file first.")

# --- PAGE 3: DATABASE VIEWER ---
elif page == "Database Viewer":
    # (This page's logic remains the same)
    logo_col, title_col = st.columns([1, 8])
    with logo_col:
        if logo_bytes:
            st.image(logo_bytes, width=150)
    with title_col:
        st.title("Database Table Viewer")
        
    st.write("Select a table from the dropdown to view its entire contents.")

    table_options = ["data", "alarm_standards", "equipment", "alarm", "component"]
    table_to_view = st.selectbox("Choose a table to display", options=table_options)

    if st.button("🔄 Refresh Table View"):
        st.cache_data.clear()
        st.rerun()

    if table_to_view:
        @st.cache_data(ttl=60)
        def view_table_data(table_name):
            try:
                with engine.connect() as connection:
                    if table_name not in table_options: 
                        st.error("Invalid table selected.")
                        return pd.DataFrame()
                    df = pd.read_sql(text(f"SELECT * FROM {table_name}"), connection)
                    
                    if table_name == 'data' and 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
                    return df
            except Exception as e:
                st.error(f"Error: Could Not Load Table '{table_name}'", icon="📋")
                st.warning("The application connected to the database but failed to retrieve data from this specific table.")
                st.info("This can happen if the table is missing or has been recently changed. Please contact the administrator.")
                return pd.DataFrame()
        
        table_df = view_table_data(table_to_view)
        
        if not table_df.empty:
            st.info(f"Displaying {len(table_df)} rows from the '{table_to_view}' table.")
            table_df = table_df.reset_index(drop=True)
            table_df.index = table_df.index + 1
            st.dataframe(
                table_df, use_container_width=True, hide_index=False,
                column_config={col: st.column_config.TextColumn(width="medium") for col in table_df.columns}
            )
        else:
            st.warning(f"The table '{table_to_view}' is empty or could not be loaded.")
