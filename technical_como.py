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
# ---    PART 1: FUNCTIONS (Logo, Database, Data Loading)
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

# --- DATA LOADING FUNCTION ---
def load_filtered_data(equipments, component, point):
    """
    Loads data from the database, filtered by component, a single point, across multiple equipments.
    """
    if not equipments or not component or not point:
        return pd.DataFrame()

    try:
        with engine.connect() as connection:
            query = text("""
                SELECT 
                    d.equipment_tag_id, d.equipment_name, d.component, d.point_measurement,
                    d.date, d.value, d.unit, d.status, d.note, d.alarm_standard,
                    stds.excellent, stds.acceptable, stds.requires_evaluation, stds.unacceptable
                FROM data d
                LEFT JOIN alarm_standards stds ON d.alarm_standard = stds.standard
                WHERE d.equipment_name IN :equipment_names
                  AND d.component = :component
                  AND d.point_measurement = :point_name
                  AND d.value IS NOT NULL
                ORDER BY d.date DESC
            """)
            
            params = {
                "equipment_names": tuple(equipments),
                "component": component,
                "point_name": point
            }
            
            df = pd.read_sql(query, connection, params=params)
            
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True)
        return df

    except Exception as e:
        st.error("Error: Failed to Load Filtered Monitoring Data", icon="📊")
        return pd.DataFrame()

# --- HELPER FUNCTION FOR COLUMN MAPPING ---
def map_and_clean_columns(df):
    COLUMN_MAPPING = {
        'identifier': 'identifier', 'equipment_tag_id': 'equipment_tag_id',
        'equipment_name': 'equipment_name', 'technology': 'technology',
        'component': 'component', 'key': 'key', 'alarm_standard': 'alarm_standard',
        'date': 'date', 'point_measurement': 'point_measurement', 'value': 'value',
        'unit': 'unit', 'status': 'status', 'excellent': 'excellent',
        'acceptable': 'acceptable', 'alarm_yellow_warning': 'requires_evaluation',
        'unacceptable_alarm': 'unacceptable', 'note': 'note',
    }
    rename_dict = {}
    ignored_columns = []
    for col in df.columns:
        normalized_col = str(col).lower().strip().replace(' ', '_').replace('(', '').replace(')', '')
        if normalized_col in COLUMN_MAPPING:
            rename_dict[col] = COLUMN_MAPPING[normalized_col]
        else:
            ignored_columns.append(col)
    if 'identifier' not in rename_dict.values():
        df['identifier'] = [f"generated_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}_{i}" for i in range(len(df))]
        st.info("Note: 'identifier' column was not found and has been auto-generated.", icon="🤖")
    df.rename(columns=rename_dict, inplace=True)
    if ignored_columns:
        st.warning(f"The following columns were found in the file but will be ignored: {', '.join(ignored_columns)}", icon="⚠️")
    return df

# --- ==================================================================== ---
# ---   PART 2: APP INITIALIZATION & MAIN LOGIC
# --- ==================================================================== ---

logo_bytes = load_logo_from_repo()
engine = get_engine()
if engine is None:
    st.error("Stopping application because a database connection could not be established.")
    st.stop()

st.sidebar.title("Navigation")
page = st.sidebar.radio("Choose a page", ["Monitoring Dashboard", "Upload New Data", "Database Viewer"])

# --- ==================================================================== ---
# ---   PAGE 1: MONITORING DASHBOARD (Component -> Point -> Equipments)
# --- ==================================================================== ---
if page == "Monitoring Dashboard":
    logo_col, title_col = st.columns([1, 8])
    with logo_col:
        if logo_bytes: st.image(logo_bytes, width=150)
    with title_col:
        st.title("Technical Condition Monitoring Dashboard")

    # --- FILTER FUNCTIONS FOR THE NEW HIERARCHY ---
    @st.cache_data(ttl=300)
    def get_all_component_options():
        """Gets a list of all unique components."""
        try:
            with engine.connect() as connection:
                df = pd.read_sql("SELECT DISTINCT component FROM data WHERE component IS NOT NULL ORDER BY component", connection)
                return df['component'].tolist()
        except Exception:
            st.error("Could not load component options.")
            return []

    @st.cache_data(ttl=300)
    def get_points_for_component(component):
        """Gets measurement points available for a given component."""
        if not component: return []
        try:
            with engine.connect() as connection:
                query = text("SELECT DISTINCT point_measurement FROM data WHERE component = :comp_name AND point_measurement IS NOT NULL ORDER BY point_measurement")
                df = pd.read_sql(query, connection, params={"comp_name": component})
                return df['point_measurement'].tolist()
        except Exception: return []
        
    @st.cache_data(ttl=300)
    def get_equipment_for_filters(component, point):
        """Gets equipment that has the selected component and point."""
        if not component or not point: return []
        try:
            with engine.connect() as connection:
                query = text("SELECT DISTINCT equipment_name FROM data WHERE component = :comp_name AND point_measurement = :point_name AND equipment_name IS NOT NULL ORDER BY equipment_name")
                df = pd.read_sql(query, connection, params={"comp_name": component, "point_name": point})
                return df['equipment_name'].tolist()
        except Exception: return []

    # --- Build the filter widgets ---
    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        component_options = get_all_component_options()
        component_choice = st.selectbox("1. Select Component", options=component_options, index=0 if component_options else None)

    with col2:
        point_options = get_points_for_component(component_choice)
        point_choice = st.selectbox("2. Select Measurement Point", options=point_options, index=0 if point_options else None)

    with col3:
        equipment_options = get_equipment_for_filters(component_choice, point_choice)
        equipment_choices = st.multiselect("3. Select Equipment(s) to Compare", options=equipment_options)

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # --- LOAD & DISPLAY DATA BASED ON SELECTIONS ---
    if equipment_choices and component_choice and point_choice:
        filtered_df = load_filtered_data(equipment_choices, component_choice, point_choice)
        
        if filtered_df.empty:
            st.warning("⚠️ No data available for the selected filters.")
            st.stop()
        
        st.header(f"Comparing: {component_choice} - {point_choice}")
        
        # --- Charting Logic ---
        professional_color_palette = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']
        plot_df = filtered_df.sort_values(by="date")
        unique_equipments = sorted(plot_df['equipment_name'].unique())
        color_map = {equip: professional_color_palette[i % len(professional_color_palette)] for i, equip in enumerate(unique_equipments)}

        fig = px.line(plot_df, x="date", y="value", color="equipment_name", markers=True, title="Trend Comparison Across Equipment", color_discrete_map=color_map)
        
        fig.update_layout(
            legend_title="Equipment", 
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.2, # Adjust this value to add more or less space below the chart
                xanchor="center",
                x=0.5
            )
        )
        
        notes_df = plot_df.dropna(subset=['note'])
        notes_df = notes_df[~notes_df['note'].astype(str).str.strip().isin(['', '-'])]
        
        for _, row in notes_df.iterrows():
            equip_name = str(row['equipment_name']).strip()
            line_color = color_map.get(equip_name)
            solid_color, transparent_color = ('rgb(200,200,200)', 'rgba(200,200,200,0.5)')
            if line_color:
                r, g, b = tuple(int(line_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
                solid_color, transparent_color = (f'rgb({r},{g},{b})', f'rgba({r},{g},{b},0.5)')
            fig.add_shape(type="line", x0=row['date'], y0=0, x1=row['date'], y1=1, yref='paper', line=dict(color=transparent_color, width=1, dash="dot"))
            fig.add_annotation(x=row['date'], y=1.05, yref='paper', text=f"<b>{row['note']}</b><br>({row['equipment_name']})", showarrow=False, font=dict(size=10, color=solid_color), xanchor="center", align="center")
        st.plotly_chart(fig, use_container_width=True)

        # --- Alarm Standards Table ---
        st.subheader("Alarm Standards")
        alarm_cols = ["equipment_name", "point_measurement", "alarm_standard", "excellent", "acceptable", "requires_evaluation", "unacceptable", "unit"]
        alarm_df = filtered_df[alarm_cols].drop_duplicates().reset_index(drop=True)
        alarm_df.index = alarm_df.index + 1
        st.dataframe(alarm_df, use_container_width=True, hide_index=False)
        
        # --- Historical Data Tables ---
        def color_status(val):
            val_lower = str(val).lower()
            if "excellent" in val_lower: return "background-color: rgba(0,128,0,0.7); color: white;"
            elif "acceptable" in val_lower: return "background-color: rgba(50,205,50,0.7); color: black;"
            elif "requires evaluation" in val_lower: return "background-color: rgba(255,165,0,0.7); color: black;"
            elif "unacceptable" in val_lower: return "background-color: rgba(255,0,0,0.7); color: white;"
            return ""

        st.header("Detailed Historical Data")
        for equipment in sorted(equipment_choices):
            st.subheader(f"History for: {equipment}")
            point_df = filtered_df[filtered_df['equipment_name'] == equipment]
            if not point_df.empty:
                hist_cols = ["date", "value", "unit", "status", "note"]
                historical_df = point_df[hist_cols].sort_values(by="date", ascending=False).reset_index(drop=True)
                historical_df.index = historical_df.index + 1
                historical_df['date'] = historical_df['date'].dt.strftime('%Y-%m-%d %H:%M')
                st.dataframe(historical_df.style.format({'value': '{:g}'}).applymap(color_status, subset=['status']), use_container_width=True, hide_index=False)
            else: 
                st.info(f"No historical data to display for {equipment}.")
            st.markdown("---")
    else:
        st.info("ℹ️ Please select a component, a measurement point, and at least one equipment to see the data.")

# --- ==================================================================== ---
# ---   PAGE 2: UPLOAD NEW DATA
# --- ==================================================================== ---
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
                    st.error("Unsupported file type.", icon="📄")
                    st.stop()
                st.info("Attempting to map file columns to database format...")
                upload_df = map_and_clean_columns(upload_df)
                st.write("Preview of data after column mapping:", upload_df.head())
                if 'identifier' in upload_df.columns:
                    upload_df.dropna(subset=['identifier'], inplace=True)
                else:
                    st.error("Upload Failed: Critical 'identifier' column is missing after mapping.", icon="❌")
                    st.stop()
                if upload_df.empty:
                    st.error("Upload Failed: No valid data found after initial cleaning.", icon="❌")
                    st.stop()
                st.info(f"Verifying columns for the '{target_table}' table...")
                with engine.connect() as connection:
                    db_cols = pd.read_sql(text(f"SELECT * FROM {target_table} LIMIT 0"), connection).columns.tolist()
                final_upload_df = upload_df[[col for col in db_cols if col in upload_df.columns]]
                st.info(f"All checks passed. Appending {len(final_upload_df)} valid rows to '{target_table}'...")
                with engine.connect() as connection:
                    final_upload_df.to_sql(target_table, con=connection, if_exists='append', index=False)
                st.success(f"Successfully added {len(final_upload_df)} rows to the '{target_table}' table!", icon="🎉")
                st.info("Clearing data cache... The dashboard will show the new data on its next load.")
                st.cache_data.clear()
            except Exception as upload_error:
                st.error("An Unexpected Error Occurred During Upload", icon="🔥")
                st.warning("This could be due to issues like incorrect data types or other formatting problems.")
                with st.expander("Show Error Details for Administrator"):
                    st.code(traceback.format_exc())
        else:
            st.warning("⚠️ Please select a table and upload a file first.")

# --- ==================================================================== ---
# ---   PAGE 3: DATABASE VIEWER
# --- ==================================================================== ---
elif page == "Database Viewer":
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
                    df = pd.read_sql(text(f"SELECT * FROM {table_name}"), connection)
                    if table_name == 'data' and 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
                    return df
            except Exception as e:
                st.error(f"Error: Could Not Load Table '{table_name}'", icon="📋")
                return pd.DataFrame()
        
        table_df = view_table_data(table_to_view)
        
        if not table_df.empty:
            st.info(f"Displaying {len(table_df)} rows from the '{table_to_view}' table.")
            table_df.index = table_df.index + 1
            st.dataframe(table_df, use_container_width=True, hide_index=False)
        else:
            st.warning(f"The table '{table_to_view}' is empty or could not be loaded.")
