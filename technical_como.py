import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import traceback
import plotly.express as px  # <-- ADDED for the new dashboard page

# --- Page Configuration ---
st.set_page_config(layout="wide")

# --- Placeholder function for the logo ---
# The new code calls display_logo(), so we must define it.
# You can replace this placeholder with your actual logo.
@st.cache_resource
def display_logo():
    """Placeholder for the logo display function"""
    # Example: st.image("your_logo.png", width=100)
    st.markdown("**(LOGO)**", help="This is a placeholder for the display_logo() function.")

# --- ==================================================================== ---
# ---    PART 1: DATABASE LOGIC
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

# --- THIS IS THE NEW/MODIFIED load_data FUNCTION ---
@st.cache_data(ttl=300) # MODIFIED: Cache for 5 mins
def load_data():
    """Loads data from the database (MODIFIED WITH LIMIT)"""
    try:
        with engine.connect() as connection:
            # MODIFIED QUERY: Added ORDER BY and LIMIT 1000
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
        st.error("Failed to load data from the database.")
        st.code(traceback.format_exc())
        return pd.DataFrame()

# --- ==================================================================== ---
# ---    PART 2: (Original simple test app logic is removed)
# --- ==================================================================== ---


# --- ==================================================================== ---
# ---    PART 3: YOUR STREAMLIT APP (This is the new multi-page logic)
# --- ==================================================================== ---

# --- Sidebar for Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Choose a page", ["Monitoring Dashboard", "Upload New Data", "Database Viewer"])


# --- PAGE 1: DASHBOARD ---
if page == "Monitoring Dashboard":
    col1_title, col2_title = st.columns([1, 10])
    with col1_title:
        display_logo()  # <-- This is the new part
    with col2_title:
        st.title("Technical Condition Monitoring Dashboard")
    
    df = load_data()  # <-- This uses the new load_data function
    
    if df.empty:
        st.warning("⚠️ No data available to display. (Or database connection failed)")
        st.stop()

    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)
    with col1:
        equipment_choice = st.selectbox("Equipment", options=sorted(df["equipment_name"].dropna().unique()))
    filtered_by_eq = df[df["equipment_name"] == equipment_choice]
    with col2:
        component_choice = st.selectbox("Component", options=sorted(filtered_by_eq["component"].dropna().unique()))
    component_df = filtered_by_eq[filtered_by_eq["component"] == component_choice]
    with col3:
        point_choices = st.multiselect("Measurement Point(s)", options=sorted(component_df["point_measurement"].dropna().unique()))

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.cache_resource.clear() # Clear both caches
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
                x=row['date'], y=1.05, yref='paper', text=f"{row['note']}<br><b>({row['point_measurement']})</b>",
                showarrow=False, font=dict(size=10, color=solid_color), xanchor="center", align="center"
            )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Alarm Standards")
        alarm_cols = ["point_measurement", "equipment_tag_id", "alarm_standard", "excellent", "acceptable", "requires_evaluation", "unacceptable", "unit"]
        alarm_df = filtered_df[alarm_cols].drop_duplicates().reset_index(drop=True)
        alarm_df.index = range(1, len(alarm_df) + 1)
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
            col1_hist, col2_hist = st.columns([2, 1])
            with col1_hist:
                st.subheader(f"History for: {point}")
            point_df = filtered_df[filtered_df['point_measurement'] == point].copy()
            if not point_df.empty:
                min_date, max_date = point_df['date'].min().date(), point_df['date'].max().date()
                with col2_hist:
                    selected_date_range = st.date_input(
                        "Filter date range", value=(min_date, max_date), min_value=min_date,
                        max_value=max_date, key=f"date_range_{point}", label_visibility="collapsed"
                    )
                if len(selected_date_range) == 2:
                    start_date, end_date = selected_date_range
                    mask = (point_df['date'].dt.date >= start_date) & (point_df['date'].dt.date <= end_date)
                    display_df = point_df.loc[mask]
                    hist_cols = ["date", "value", "unit", "status", "note"]
                    historical_df = display_df[hist_cols].sort_values(by="date", ascending=False)
                    if not historical_df.empty:
                        historical_df['date'] = historical_df['date'].dt.strftime('%Y-%m-%d')
                        historical_df = historical_df.reset_index(drop=True)
                        historical_df.index = range(1, len(historical_df) + 1)
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
                    else: st.info("No data available for the selected date range.")
                else: st.warning("Please select a valid date range (start and end date).")
            else: st.info("No historical data to display for this point.")
            st.markdown("---")
    else:
        st.info("ℹ️ Please select one or more measurement points from the filters above to see the data.")

# --- PAGE 2: UPLOAD DATA ---
elif page == "Upload New Data":
    col1_title, col2_title = st.columns([1, 10])
    with col1_title:
        display_logo()  # <-- This is the new part
    with col2_title:
        st.title("Upload New Data")
    st.write("Use this page to add new records to the database tables from a CSV or XLSX file.")
    
    table_options = ["data", "alarm_standards", "equipment", "alarm", "component"]
    target_table = st.selectbox("1. Select table to add data to", options=table_options)
    uploaded_file = st.file_uploader("2. Choose a file", type=["csv", "xlsx"])

    if st.button("3. Upload and Add Data"):
        if uploaded_file is not None and target_table is not None:
            try:
                upload_df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file, engine='openpyxl')
                st.write("Preview of original uploaded data:"); st.dataframe(upload_df.head())

                if 'identifier' in upload_df.columns:
                    initial_row_count = len(upload_df)
                    upload_df.dropna(subset=['identifier'], inplace=True)
                    rows_removed = initial_row_count - len(upload_df)
                    if rows_removed > 0:
                        st.warning(f"Found and removed {rows_removed} row(s) with a blank 'identifier'. These rows were not processed.")
                
                if upload_df.empty:
                    st.error("Upload Failed: After removing blank rows, no valid data remains. Please check your file.")
                    st.stop()

                st.info(f"Checking columns for the '{target_table}' table...")
                with engine.connect() as connection:
                    db_cols = pd.read_sql(text(f"SELECT * FROM {target_table} LIMIT 0"), connection).columns.tolist()
                upload_cols = upload_df.columns.tolist()
                db_cols_set, upload_cols_set = set(db_cols), set(upload_cols)
                if upload_cols_set != db_cols_set:
                    st.error(f"Column Mismatch! The file columns do not match the '{target_table}' table.")
                    missing, extra = list(db_cols_set - upload_cols_set), list(upload_cols_set - db_cols_set)
                    if missing: st.warning("**Columns missing from your file:**"); st.json(sorted(missing))
                    if extra: st.warning("**Unexpected columns found in your file:**"); st.json(sorted(extra))
                    st.info("For reference:")
                    st.write("**Full list of expected columns:**", sorted(db_cols))
                    st.write("**Full list of your file's columns:**", sorted(upload_cols))
                    st.stop()
                
                unique_key_map = {'data': 'identifier', 'alarm_standards': 'standard', 'component': 'point'}
                unique_key = unique_key_map.get(target_table)
                if unique_key and unique_key in upload_df.columns:
                    st.info(f"Checking for duplicate '{unique_key}' values in the database...")
                    upload_ids = upload_df[unique_key].dropna().tolist()
                    if upload_ids:
                        with engine.connect() as connection:
                            query = text(f'SELECT "{unique_key}" FROM "{target_table}" WHERE "{unique_key}" IN :ids')
                            existing_ids_df = pd.read_sql(query, connection, params={'ids': tuple(upload_ids)})
                        existing_ids = set(existing_ids_df[unique_key])
                        duplicate_ids = [id for id in upload_ids if id in existing_ids]
                        if duplicate_ids:
                            st.error(f"Upload Failed: Found {len(duplicate_ids)} rows in your file where the '{unique_key}' already exists in the database.")
                            st.warning(f"The '{unique_key}' column must be unique. Please remove or update these rows:")
                            st.json(sorted(list(set(duplicate_ids))))
                            st.stop()

                st.info(f"All checks passed. Appending {len(upload_df)} valid rows to '{target_table}'...")
                with engine.connect() as connection:
                    upload_df.to_sql(target_table, con=connection, if_exists='append', index=False)
                st.success(f"Successfully added {len(upload_df)} rows to the '{target_table}' table!")
                st.info("Clearing data cache... The dashboard will show the new data on its next load.")
                st.cache_data.clear()

            except Exception as upload_error:
                st.error("An error occurred during the upload process:"); st.code(traceback.format_exc())
        else:
            st.warning("⚠️ Please select a table and upload a file first.")

# --- PAGE 3: DATABASE VIEWER ---
elif page == "Database Viewer":
    col1_title, col2_title = st.columns([1, 10])
    with col1_title:
        display_logo()  # <-- This is the new part
    with col2_title:
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
                    if table_name not in table_options: st.error("Invalid table selected."); return pd.DataFrame()
                    df = pd.read_sql(text(f"SELECT * FROM {table_name}"), connection)
                    
                    if table_name == 'data' and 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')

                    return df
            except Exception as e:
                st.error(f"Failed to load data from table '{table_name}'."); st.code(traceback.format_exc()); return pd.DataFrame()
        
        table_df = view_table_data(table_to_view)
        
        if not table_df.empty:
            st.info(f"Displaying {len(table_df)} rows from the '{table_to_view}' table.")
            table_df = table_df.reset_index(drop=True)
            table_df.index = range(1, len(table_df) + 1)
            st.dataframe(
                table_df, use_container_width=True,
                column_config={col: st.column_config.TextColumn(width="medium") for col in table_df.columns}
            )
        else:
            st.warning(f"The table '{table_to_view}' is empty or could not be loaded.")
