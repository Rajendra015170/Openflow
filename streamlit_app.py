import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

# Page configuration
st.set_page_config(
    page_title="ZDQ - Data Quality Hub",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 1rem;
        border-left: 4px solid #3498db;
        padding-left: 1rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        text-align: center;
        margin: 1rem 0;
    }
    
    .success-card {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(40, 167, 69, 0.3);
    }
    
    .failure-card {
        background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 15px rgba(220, 53, 69, 0.3);
    }
    
    .info-box {
        background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        margin: 1rem 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    .stSelectbox > div > div {
        background-color: #f8f9fa;
        border-radius: 10px;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.3);
    }
    
    .nav-card {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        border-left: 4px solid #3498db;
        margin: 1rem 0;
        transition: transform 0.3s ease;
    }
    
    .nav-card:hover {
        transform: translateY(-5px);
    }
    
    /* Enhanced table styling with Brazilian colors */
    .dataframe {
        border-collapse: collapse;
        margin: 1rem 0;
        font-size: 0.9rem;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    
    .dataframe th {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        padding: 1rem;
        text-align: center;
        border: none;
    }
    
    .dataframe td {
        padding: 0.8rem;
        text-align: center;
        border-bottom: 1px solid #e9ecef;
    }
    
    /* Brazilian light colors for alternating rows */
    .dataframe tbody tr:nth-child(odd) {
        background-color: #fff8e1; /* Light warm yellow */
    }
    
    .dataframe tbody tr:nth-child(even) {
        background-color: #e8f5e8; /* Light green */
    }
    
    .dataframe tbody tr:hover {
        background-color: #e3f2fd; /* Light blue on hover */
        transform: scale(1.02);
        transition: all 0.3s ease;
    }
    
    /* Status-specific styling */
    .status-success {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%) !important;
        color: white !important;
        font-weight: bold !important;
        border-radius: 20px !important;
        padding: 0.5rem 1rem !important;
        margin: 0.2rem !important;
        box-shadow: 0 2px 8px rgba(40, 167, 69, 0.3) !important;
    }
    
    .status-failure {
        background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%) !important;
        color: white !important;
        font-weight: bold !important;
        border-radius: 20px !important;
        padding: 0.5rem 1rem !important;
        margin: 0.2rem !important;
        box-shadow: 0 2px 8px rgba(220, 53, 69, 0.3) !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Snowflake session
@st.cache_resource
def init_snowflake_session():
    try:
        session = get_active_session()
        return session, True
    except:
        return None, False

session, session_connected = init_snowflake_session()

if not session_connected:
    st.error("❌ Could not establish Snowflake connection. Please ensure you're properly connected.")
    st.stop()

# Environment to database mapping
ENV_DB_MAP = {
    "DEV": "dev_db_manager",
    "QA": "qa_db_manager", 
    "UAT": "uat_db_manager",
    "PROD": "prod_db_manager"
}

# Sidebar navigation with icons
st.sidebar.markdown("### 🎯 ZDQ Navigation")
page = st.sidebar.radio(
    "Choose your validation process:",
    ["🏠 Home", "📊 Data Ingestion DQ", "🎭 Masking DQ", "🔐 Encryption DQ"],
    label_visibility="collapsed"
)

# Data fetching functions with caching
@st.cache_data(ttl=300)
def fetch_list(query):
    if session:
        try:
            return [row['name'] for row in session.sql(query).collect()]
        except:
            return []
    return []

@st.cache_data(ttl=300)
def fetch_databases(environment):
    return fetch_list("SHOW DATABASES") if session else []

@st.cache_data(ttl=300)
def fetch_schemas(database_name):
    return fetch_list(f"SHOW SCHEMAS IN DATABASE {database_name}") if session else []

@st.cache_data(ttl=300)
def fetch_source_db_types(environment):
    db_name = ENV_DB_MAP.get(environment)
    if not db_name: return []
    query = f"SELECT DISTINCT db_type FROM {db_name}.public.audit_recon WHERE db_type IS NOT NULL"
    try:
        return [row['DB_TYPE'] for row in session.sql(query).collect()] if session else []
    except:
        return []

@st.cache_data(ttl=300)
def fetch_load_groups(environment):
    db_name = ENV_DB_MAP.get(environment)
    if not db_name: return []
    query = f"SELECT DISTINCT LOAD_GROUP FROM {db_name}.public.audit_recon"
    try:
        return [row['LOAD_GROUP'] for row in session.sql(query).collect()] if session else []
    except:
        return []

def get_source_target_tables(load_group, load_type, source_db_type, environment):
    db_name = ENV_DB_MAP.get(environment)
    if not db_name: return [], []

    query_template = lambda db_type: f"""
        SELECT upper(table_name) as table_name, row_count FROM {db_name}.public.audit_recon
        WHERE LOAD_GROUP IN ('{load_group}') AND LOAD_TYPE IN ('{load_type}') AND db_type = '{db_type}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_NAME ORDER BY ROW_CRE_DT) = 1
    """

    try:
        source_result = session.sql(query_template(source_db_type)).collect() if session else []
        target_result = session.sql(query_template('SNOWFLAKE')).collect() if session else []

        source_tables = [{'table_name': r['TABLE_NAME'], 'row_count': r['ROW_COUNT']} for r in source_result]
        target_tables = [{'table_name': r['TABLE_NAME'], 'row_count': r['ROW_COUNT']} for r in target_result]

        source_tables.sort(key=lambda x: x['table_name'])
        target_tables.sort(key=lambda x: x['table_name'])

        return source_tables, target_tables
    except Exception as e:
        st.error(f"Error fetching tables: {e}")
        return [], []

def run_count_validation(selected_load_group, load_type, source_db_type, environment):
    with st.spinner("🔄 Running count validation..."):
        s_list, t_list = get_source_target_tables(selected_load_group, load_type, source_db_type, environment)
        max_len = max(len(s_list), len(t_list))
        rows = []

        for i in range(max_len):
            s = s_list[i] if i < len(s_list) else {'table_name': 'N/A', 'row_count': 0}
            t = t_list[i] if i < len(t_list) else {'table_name': 'N/A', 'row_count': 0}
            test_result = "SUCCESS" if s['row_count'] == t['row_count'] else "FAILURE"

            detail_msg = ""
            if test_result == "FAILURE":
                if s['row_count'] > t['row_count']:
                    detail_msg = "Source count is greater than target count"
                elif t['row_count'] > s['row_count']:
                    detail_msg = "Target count is greater than source count"

            rows.append({
                "Load Type": load_type,
                "Load Group": selected_load_group,
                "Environment": environment,
                "SOURCE_TABLE": s['table_name'],
                "SOURCE_ROWS": s['row_count'],
                "TARGET_TABLE": t['table_name'],
                "TARGET_ROWS": t['row_count'],
                "Test Case": test_result,
                "Details": detail_msg
            })

        df = pd.DataFrame(rows)
        return df

def run_data_validation(selected_db, selected_schema, load_type, selected_load_group, environment):
    with st.spinner("🔄 Running data validation..."):
        query_tables = f"""
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
            FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{selected_schema}'
            AND TABLE_NAME IN (
                SELECT DISTINCT UPPER(TABLE_NAME)
                FROM {ENV_DB_MAP[environment]}.public.audit_recon
                WHERE LOAD_GROUP IN ('{selected_load_group}')
                AND LOAD_TYPE IN ('{load_type}')
            )
            AND COLUMN_NAME NOT LIKE 'ROW_%'
            AND COLUMN_NAME NOT LIKE 'RAW_%'
        """

        try:
            tables_df = session.sql(query_tables).to_pandas()
            if tables_df.empty:
                st.info("ℹ️ No tables found for the given criteria.")
                return pd.DataFrame([])

            results = []
            env_datalake_map = {
                "DEV": "dev_datalake",
                "QA": "qa_datalake",
                "UAT": "uat_datalake",
                "PROD": "prod_datalake"
            }

            progress_bar = st.progress(0)
            for idx, (_, row) in enumerate(tables_df.iterrows()):
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                
                progress_bar.progress((idx + 1) / len(tables_df))

                source_db_name = env_datalake_map.get(environment, f"{selected_db}_RAW")

                try:
                    # Target vs View
                    target_vs_view_query = f"""
                        SELECT COUNT(*) AS DIFF_COUNT FROM (
                            SELECT * EXCLUDE (ROW_CRE_DT, ROW_MOD_DT, ROW_CRE_USR_ID, ROW_MOD_USR_ID, RAW_ROW_CRE_DT)
                            FROM {selected_db}.{schema_name}.{table_name}
                            MINUS
                            SELECT DISTINCT * EXCLUDE (RAW_ROW_CRE_DT)
                            FROM {source_db_name}.{schema_name}.VW_RAW_{table_name}
                        )
                    """
                    t2v_result = session.sql(target_vs_view_query).collect()
                    t2v_diff = t2v_result[0]['DIFF_COUNT'] if t2v_result else 0

                    # View vs Target
                    view_vs_target_query = f"""
                        SELECT COUNT(*) AS DIFF_COUNT FROM (
                            SELECT DISTINCT * EXCLUDE (RAW_ROW_CRE_DT)
                            FROM {source_db_name}.{schema_name}.VW_RAW_{table_name}
                            MINUS
                            SELECT * EXCLUDE (ROW_CRE_DT, ROW_MOD_DT, ROW_CRE_USR_ID, ROW_MOD_USR_ID, RAW_ROW_CRE_DT)
                            FROM {selected_db}.{schema_name}.{table_name}
                        )
                    """
                    v2t_result = session.sql(view_vs_target_query).collect()
                    v2t_diff = v2t_result[0]['DIFF_COUNT'] if v2t_result else 0

                except Exception as e:
                    st.warning(f"⚠️ Error comparing {schema_name}.{table_name}: {e}")
                    t2v_diff = v2t_diff = -1

                test_case_result = "SUCCESS" if t2v_diff == 0 and v2t_diff == 0 else "FAILURE"

                results.append({
                    "Load Type": load_type,
                    "Load Group": selected_load_group,
                    "Environment": environment,
                    "Database": selected_db,
                    "Schema": schema_name,
                    "Table": table_name,
                    "TARGET VS VIEW": t2v_diff,
                    "VIEW VS TARGET": v2t_diff,
                    "Test Case": test_case_result
                })

            progress_bar.empty()
            return pd.DataFrame(results)
        except Exception as e:
            st.error(f"❌ Error during data validation: {e}")
            return pd.DataFrame([])

def run_duplicate_validation(selected_db, selected_schema, load_type, selected_load_group, environment):
    with st.spinner("🔄 Running duplicate validation..."):
        query_tables = f"""
            SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
            FROM {selected_db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{selected_schema}'
            AND TABLE_NAME IN (
                SELECT DISTINCT UPPER(TABLE_NAME)
                FROM {ENV_DB_MAP[environment]}.public.audit_recon
                WHERE LOAD_GROUP IN ('{selected_load_group}')
                AND LOAD_TYPE IN ('{load_type}')
            )
            AND COLUMN_NAME NOT LIKE 'ROW_%'
            AND COLUMN_NAME NOT LIKE 'RAW_%'
        """

        try:
            tables_df = session.sql(query_tables).to_pandas()
            if tables_df.empty:
                st.info("ℹ️ No tables found for the given criteria.")
                return pd.DataFrame([])

            results = []
            progress_bar = st.progress(0)

            for idx, (_, row) in enumerate(tables_df.iterrows()):
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                
                progress_bar.progress((idx + 1) / len(tables_df))

                try:
                    dup_query = f"""
                        SELECT COUNT(*) AS DUP_COUNT FROM (
                            SELECT * EXCLUDE (ROW_CRE_DT, ROW_MOD_DT, ROW_CRE_USR_ID, ROW_MOD_USR_ID, RAW_ROW_CRE_DT)
                            FROM {selected_db}.{schema_name}.{table_name}
                            GROUP BY ALL
                            HAVING COUNT(*) > 1
                        )
                    """
                    result = session.sql(dup_query).collect()
                    dup_count = result[0]['DUP_COUNT'] if result else 0
                    test_case_result = "SUCCESS" if dup_count == 0 else "FAILURE"
                except Exception as e:
                    st.warning(f"⚠️ Error checking duplicates in {schema_name}.{table_name}: {e}")
                    dup_count = -1
                    test_case_result = "FAILURE"

                results.append({
                    "Load Type": load_type,
                    "Load Group": selected_load_group,
                    "Environment": environment,
                    "Database": selected_db,
                    "Schema": schema_name,
                    "Table": table_name,
                    "DUP COUNT": dup_count,
                    "Test Case": test_case_result
                })

            progress_bar.empty()
            return pd.DataFrame(results)
        except Exception as e:
            st.error(f"❌ Error during duplicate validation: {e}")
            return pd.DataFrame([])

def style_dataframe(df):
    """Apply beautiful styling to dataframes with Brazilian colors and enhanced status styling"""
    def highlight_test_case(val):
        if val == "SUCCESS":
            return 'background: linear-gradient(135deg, #28a745 0%, #20c997 100%); color: GREEN; font-weight: bold; border-radius: 20px; padding: 0.5rem 1rem; text-align: center; box-shadow: 0 2px 8px rgba(40, 167, 69, 0.3);'
        elif val == "FAILURE":
            return 'background: linear-gradient(135deg, #dc3545 0%, #fd7e14 100%); color: RED; font-weight: bold; border-radius: 20px; padding: 0.5rem 1rem; text-align: center; box-shadow: 0 2px 8px rgba(220, 53, 69, 0.3);'
        return ''
    
    def highlight_rows(row):
        # Brazilian-inspired alternating colors
        colors = ['background-color: #fff8e1;', 'background-color: #e8f5e8;']  # Light warm yellow and light green
        return [colors[row.name % 2]] * len(row)
    
    if 'Test Case' in df.columns:
        styled = df.style.apply(highlight_rows, axis=1).applymap(highlight_test_case, subset=['Test Case'])
        return styled.set_table_styles([
            {'selector': 'th', 'props': [
                ('background', 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'),
                ('color', 'black'),
                ('font-weight', 'bold'),
                ('text-align', 'center'),
                ('padding', '1rem'),
                ('border', 'none')
            ]},
            {'selector': 'td', 'props': [
                ('text-align', 'center'),
                ('padding', '0.8rem'),
                ('border-bottom', '1px solid #e9ecef')
            ]},
            {'selector': 'table', 'props': [
                ('border-collapse', 'collapse'),
                ('border-radius', '10px'),
                ('overflow', 'hidden'),
                ('box-shadow', '0 4px 15px rgba(0,0,0,0.1)')
            ]}
        ])
    else:
        return df.style.apply(highlight_rows, axis=1).set_table_styles([
            {'selector': 'th', 'props': [
                ('background', 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'),
                ('color', 'black'),
                ('font-weight', 'bold'),
                ('text-align', 'center'),
                ('padding', '1rem'),
                ('border', 'none')
            ]},
            {'selector': 'td', 'props': [
                ('text-align', 'center'),
                ('padding', '0.8rem'),
                ('border-bottom', '1px solid #e9ecef')
            ]},
            {'selector': 'table', 'props': [
                ('border-collapse', 'collapse'),
                ('border-radius', '10px'),
                ('overflow', 'hidden'),
                ('box-shadow', '0 4px 15px rgba(0,0,0,0.1)')
            ]}
        ])

def display_summary_metrics(df):
    """Display summary metrics with beautiful cards"""
    if df.empty:
        return
    
    total_tests = len(df)
    success_count = len(df[df['Test Case'] == 'SUCCESS']) if 'Test Case' in df.columns else 0
    failure_count = total_tests - success_count
    success_rate = (success_count / total_tests * 100) if total_tests > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📊 Total Tests</h3>
            <h2>{total_tests}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="success-card">
            <h3>✅ Passed</h3>
            <h2>{success_count}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="failure-card">
            <h3>❌ Failed</h3>
            <h2>{failure_count}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🎯 Success Rate</h3>
            <h2>{success_rate:.1f}%</h2>
        </div>
        """, unsafe_allow_html=True)

# Initialize session state
if 'load_group' not in st.session_state:
    st.session_state['load_group'] = None

# Main application logic
if page == "🏠 Home":
    st.markdown('<h1 class="main-header">🎯 Welcome to ZDQ Hub</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-box">
        <h3>🚀 Your Data Quality Command Center</h3>
        <p>Automate and streamline your data quality processes with our comprehensive validation suite. 
        Navigate through different validation types using the sidebar to ensure your data integrity across all environments.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="nav-card">
            <h3>📊 Data Ingestion DQ</h3>
            <p>Validate data ingestion processes including count validation, data integrity checks, and duplicate detection.</p>
            <ul>
                <li>✅ Count Validation</li>
                <li>🔍 Data Validation</li>
                <li>🔄 Duplicate Detection</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="nav-card">
            <h3>🎭 Masking DQ</h3>
            <p>Ensure data masking compliance and validate masked data integrity across environments.</p>
            <ul>
                <li>🏷️ Tag Validation</li>
                <li>📋 Table Validation</li>
                <li>📊 Column Validation</li>
                <li>👁️ View Validation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="nav-card">
            <h3>🔐 Encryption DQ</h3>
            <p>Validate encryption processes and ensure data security compliance.</p>
            <ul>
                <li>🔒 Column Level Validation</li>
                <li>📊 Table Level Validation</li>
                <li>🛡️ Security Compliance</li>
                <li>🔑 Environment Mapping</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

elif page == "📊 Data Ingestion DQ":
    st.markdown('<h1 class="main-header">📊 Data Ingestion Quality</h1>', unsafe_allow_html=True)
    
    # Control panel
    st.markdown('<h3 class="sub-header">🎛️ Control Panel</h3>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        environment = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"])
    with col2:
        rules = ["COUNT VALIDATION", "DATA VALIDATION", "DUPLICATE VALIDATION"]
        dq_rule = st.selectbox("📋 Validation Rule", rules)
    with col3:
        source_db_types = fetch_source_db_types(environment)
        source_db_type = st.selectbox("🗄️ Source DB Type", source_db_types if source_db_types else ["No db_type found"])

    databases = fetch_databases(environment) if dq_rule != "COUNT VALIDATION" else []
    selected_db = ""
    selected_schema = ""

    col4, col5, col6 = st.columns([1.2, 1, 1])
    with col4:
        if dq_rule != "COUNT VALIDATION":
            selected_db = st.selectbox("🏢 Database", databases if databases else ["No databases found"])
    with col5:
        if dq_rule != "COUNT VALIDATION" and selected_db:
            schemas = fetch_schemas(selected_db)
            selected_schema = st.selectbox("📁 Schema", schemas if schemas else ["No schemas found"])
    with col6:
        load_type_input = st.text_input("⚡ Load Type", "")

    load_groups = fetch_load_groups(environment)
    if st.session_state['load_group'] is None and load_groups:
        st.session_state['load_group'] = load_groups[0]
    
    if load_groups:
        selected_load_group = st.selectbox("📦 Load Group", load_groups,
                                           index=load_groups.index(st.session_state['load_group']) if st.session_state['load_group'] in load_groups else 0)
        st.session_state['load_group'] = selected_load_group
    else:
        st.warning("⚠️ No load groups found for the selected environment.")
        selected_load_group = None

    # Validation execution
    if st.button("🚀 Run Validation", type="primary"):
        if not load_type_input.strip():
            st.error("❌ Please enter a Load Type")
        elif not selected_load_group:
            st.error("❌ Please select a Load Group")
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if dq_rule == "COUNT VALIDATION":
                df = run_count_validation(selected_load_group, load_type_input.strip(), source_db_type, environment)
                if not df.empty:
                    st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                    display_summary_metrics(df)
                    
                    st.markdown("### 📋 Detailed Results")
                    styled_df = style_dataframe(df)
                    st.dataframe(styled_df, use_container_width=True)
                    
                    csv_data = df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Download Results", data=csv_data, 
                                     file_name=f"count_validation_{timestamp}.csv", mime="text/csv")
                
            elif dq_rule == "DATA VALIDATION":
                if not selected_schema:
                    st.error("❌ Please select a schema.")
                else:
                    df = run_data_validation(selected_db, selected_schema, load_type_input.strip(), selected_load_group, environment)
                    if not df.empty:
                        st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                        display_summary_metrics(df)
                        
                        st.markdown("### 📋 Detailed Results")
                        styled_df = style_dataframe(df)
                        st.dataframe(styled_df, use_container_width=True)
                        
                        csv_data = df.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Download Results", data=csv_data,
                                         file_name=f"data_validation_{timestamp}.csv", mime="text/csv")
                
            elif dq_rule == "DUPLICATE VALIDATION":
                if not selected_schema:
                    st.error("❌ Please select a schema.")
                else:
                    df = run_duplicate_validation(selected_db, selected_schema, load_type_input.strip(), selected_load_group, environment)
                    if not df.empty:
                        st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                        display_summary_metrics(df)
                        
                        st.markdown("### 📋 Detailed Results")
                        styled_df = style_dataframe(df)
                        st.dataframe(styled_df, use_container_width=True)
                        
                        csv_data = df.to_csv(index=False).encode('utf-8')
                        st.download_button("📥 Download Results", data=csv_data,
                                         file_name=f"duplicate_validation_{timestamp}.csv", mime="text/csv")

elif page == "🎭 Masking DQ":
    st.markdown('<h1 class="main-header">🎭 Data Masking Quality</h1>', unsafe_allow_html=True)
    
    # Masking DQ functions
    @st.cache_data(ttl=300)
    def get_databases(env_prefix):
        db_prefix = f"{env_prefix}_"
        db_query = f"""
            SELECT DATABASE_NAME 
            FROM INFORMATION_SCHEMA.DATABASES 
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
        """
        try:
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_schemas(database):
        schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
        try:
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_classification_owners(env):
        owner_query = f"""
            SELECT DISTINCT CLASSIFICATION_OWNER
            FROM {env}_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
        """
        try:
            rows = session.sql(owner_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    def execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner):
        try:
            production_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
            source_tags_query = f"""
            SELECT COUNT(*) AS total_records
            FROM {env}_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
            WHERE "DATABASE" = '{production_database}'
              AND "SCHEMA" = '{selected_schema}'
              AND CLASSIFICATION_OWNER = '{classification_owner}'
            """
            target_tags_query = f"""
            SELECT COUNT(*) AS TAG_COUNT
            FROM {env}_DB_MANAGER.ACCOUNT_USAGE.TAG_REFERENCES
            WHERE OBJECT_DATABASE = '{selected_database}_MASKED'
              AND OBJECT_SCHEMA = '{selected_schema}'
            """
            source_count = session.sql(source_tags_query).collect()[0][0]
            target_count = session.sql(target_tags_query).collect()[0][0]
            return source_count, target_count
        except Exception as e:
            return None, str(e)

    def execute_validation_queries_tables(env, selected_database, selected_schema):
        try:
            db_manager = f"{env}_DB_MANAGER"
            count_tables_query = f"""
            SELECT COUNT(TABLE_NAME) 
            FROM {selected_database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_CATALOG = '{selected_database}'
              AND TABLE_SCHEMA = '{selected_schema}'
              AND TABLE_TYPE = 'BASE TABLE'
              AND TABLE_NAME NOT LIKE 'RAW_%'
              AND TABLE_NAME NOT LIKE 'VW_%'
            """
            validation_query = f"""
            SELECT COUNT(*) AS TABLE_COUNT
            FROM {db_manager}.MASKING.MD_TABLE t
            JOIN {db_manager}.MASKING.MD_SCHEMA s ON t.SCHEMA_ID = s.SCHEMA_ID
            JOIN {db_manager}.MASKING.MD_DATABASE d ON s.DATABASE_ID = d.DATABASE_ID
            WHERE d.DATABASE_NAME = '{selected_database}'
              AND s.SCHEMA_NAME = '{selected_schema}'
            """
            table_count = session.sql(count_tables_query).collect()[0][0]
            validation_count = session.sql(validation_query).collect()[0][0]
            return table_count, validation_count
        except Exception as e:
            return None, str(e)

    def execute_validation_queries_columns(env, selected_database, selected_schema):
        try:
            db_manager = f"{env}_DB_MANAGER"
            count_columns_query = f"""
            SELECT COUNT(c.COLUMN_NAME) AS COLUMN_COUNT
            FROM {selected_database}.INFORMATION_SCHEMA.COLUMNS c
            JOIN {selected_database}.INFORMATION_SCHEMA.TABLES t
              ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
            WHERE c.TABLE_SCHEMA = '{selected_schema}'
              AND t.TABLE_TYPE = 'BASE TABLE'
              AND c.TABLE_NAME NOT LIKE 'RAW_%'
              AND c.TABLE_NAME NOT LIKE 'VW_%'
            """
            validation_query = f"""
            SELECT COUNT(col.COLUMN_ID) AS COLUMN_COUNT
            FROM {db_manager}.MASKING.MD_DATABASE db
            JOIN {db_manager}.MASKING.MD_SCHEMA sc ON db.DATABASE_ID = sc.DATABASE_ID
            JOIN {db_manager}.MASKING.MD_TABLE tb ON sc.SCHEMA_ID = tb.SCHEMA_ID
            JOIN {db_manager}.MASKING.MD_COLUMN col ON tb.TABLE_ID = col.TABLE_ID
            WHERE db.database_name='{selected_database}'
              AND sc.schema_name='{selected_schema}'
              AND db.IS_ACTIVE = TRUE
              AND sc.IS_ACTIVE = TRUE
              AND tb.IS_ACTIVE = TRUE
              AND col.IS_ACTIVE = TRUE
            """
            column_count = session.sql(count_columns_query).collect()[0][0]
            validation_count = session.sql(validation_query).collect()[0][0]
            return column_count, validation_count
        except Exception as e:
            return None, str(e)

    def execute_validation_queries_views(env, selected_database, selected_schema):
        try:
            count_tables_query = f"""
            SELECT COUNT(TABLE_NAME) 
            FROM {selected_database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_CATALOG = '{selected_database}'
              AND TABLE_SCHEMA = '{selected_schema}'
              AND TABLE_TYPE = 'BASE TABLE'
              AND TABLE_NAME NOT LIKE 'RAW_%'
              AND TABLE_NAME NOT LIKE 'VW_%'
            """
            count_target_query = f"""
            SELECT COUNT(TABLE_NAME) 
            FROM {selected_database}_MASKED.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{selected_schema}'
            """
            table_count = session.sql(count_tables_query).collect()[0][0]
            validation_count = session.sql(count_target_query).collect()[0][0]
            return table_count, validation_count
        except Exception as e:
            return None, str(e)

    def execute_validation_queries_data_set(env, selected_database, selected_schema):
        try:
            db_manager = f"{env}_DB_MANAGER"
            count_columns_query = f"""
            SELECT COUNT(col.COLUMN_ID) AS COLUMN_COUNT
            FROM {db_manager}.MASKING.MD_DATABASE db
            JOIN {db_manager}.MASKING.MD_SCHEMA sc ON db.DATABASE_ID = sc.DATABASE_ID
            JOIN {db_manager}.MASKING.MD_TABLE tb ON sc.SCHEMA_ID = tb.SCHEMA_ID
            JOIN {db_manager}.MASKING.MD_COLUMN col ON tb.TABLE_ID = col.TABLE_ID
            WHERE db.database_name='{selected_database}'
              AND sc.schema_name='{selected_schema}'
              AND db.IS_ACTIVE = TRUE
              AND sc.IS_ACTIVE = TRUE
              AND tb.IS_ACTIVE = TRUE
              AND col.IS_ACTIVE = TRUE
            """
            validation_query = f"""
            SELECT COUNT(*) AS total_records
            FROM (
                SELECT DISTINCT
                    ds.data_output_id,
                    d.database_name,
                    s.schema_name,
                    t.table_name,
                    c.column_name
                FROM {db_manager}.MASKING.DATA_SET ds
                INNER JOIN {db_manager}.MASKING.MD_DATABASE d ON ds.database_id = d.database_id
                INNER JOIN {db_manager}.MASKING.MD_SCHEMA s ON ds.schema_id = s.schema_id
                INNER JOIN {db_manager}.MASKING.MD_TABLE t ON ds.TABLE_ID = t.TABLE_ID
                INNER JOIN {db_manager}.MASKING.MD_COLUMN c ON ds.COLUMN_ID = c.COLUMN_ID
                WHERE d.database_name = '{selected_database}'
                  AND s.schema_name = '{selected_schema}'
                  AND ds.data_output_id = (
                      SELECT MAX(ds1.data_output_id) 
                      FROM {db_manager}.MASKING.DATA_SET ds1
                      INNER JOIN {db_manager}.MASKING.MD_DATABASE d1 ON ds1.database_id = d1.database_id
                      INNER JOIN {db_manager}.MASKING.MD_SCHEMA s1 ON ds1.schema_id = s1.schema_id
                      WHERE d1.database_name = '{selected_database}'
                        AND s1.schema_name = '{selected_schema}'
                  )
            ) AS subquery
            """
            column_count = session.sql(count_columns_query).collect()[0][0]
            data_count = session.sql(validation_query).collect()[0][0]
            return column_count, data_count
        except Exception as e:
            return None, str(e)

    # UI Controls
    st.markdown('<h3 class="sub-header">🎛️ Control Panel</h3>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        env = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"])
    with col2:
        database_list = get_databases(env)
        selected_database = st.selectbox("🏢 Database", database_list, key="db_select")
    with col3:
        schema_list = get_schemas(selected_database) if selected_database else []
        selected_schema = st.selectbox("📁 Schema", schema_list, key="schema_select")
    with col4:
        classification_owners = get_classification_owners(env)
        classification_owner = st.selectbox("👤 Classification Owner", classification_owners)

    if st.button("🚀 Run All Validations", type="primary"):
        if not all([env, selected_database, selected_schema, classification_owner]):
            st.error("❌ Please fill in all required fields")
        else:
            with st.spinner("🔄 Running comprehensive masking validations..."):
                results_for_csv = []
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Progress tracking
                validation_steps = ["MD Tables", "MD Columns", "Data Set", "Views", "Tags"]
                progress_bar = st.progress(0)
                
                for idx, validation_type in enumerate(validation_steps):
                    progress_bar.progress((idx + 1) / len(validation_steps))
                    
                    if validation_type == "MD Tables":
                        table_count, table_validation_count = execute_validation_queries_tables(env, selected_database, selected_schema)
                        test_case = "SUCCESS" if table_count == table_validation_count else "FAILURE"
                        results_for_csv.append({
                            "Environment": env,
                            "Database": selected_database,
                            "Schema": selected_schema,
                            "Validation": "MD Tables",
                            "Source Count": table_count,
                            "Target Count": table_validation_count,
                            "Test Case": test_case
                        })
                    
                    elif validation_type == "MD Columns":
                        column_count, column_validation_count = execute_validation_queries_columns(env, selected_database, selected_schema)
                        test_case = "SUCCESS" if column_count == column_validation_count else "FAILURE"
                        results_for_csv.append({
                            "Environment": env,
                            "Database": selected_database,
                            "Schema": selected_schema,
                            "Validation": "MD Columns",
                            "Source Count": column_count,
                            "Target Count": column_validation_count,
                            "Test Case": test_case
                        })
                    
                    elif validation_type == "Data Set":
                        dataset_count, dataset_data_count = execute_validation_queries_data_set(env, selected_database, selected_schema)
                        test_case = "SUCCESS" if dataset_count == dataset_data_count else "FAILURE"
                        results_for_csv.append({
                            "Environment": env,
                            "Database": selected_database,
                            "Schema": selected_schema,
                            "Validation": "Data Set",
                            "Source Count": dataset_count,
                            "Target Count": dataset_data_count,
                            "Test Case": test_case
                        })
                    
                    elif validation_type == "Views":
                        view_table_count, validation_count_views = execute_validation_queries_views(env, selected_database, selected_schema)
                        test_case = "SUCCESS" if view_table_count == validation_count_views else "FAILURE"
                        results_for_csv.append({
                            "Environment": env,
                            "Database": selected_database,
                            "Schema": selected_schema,
                            "Validation": "Views",
                            "Source Count": view_table_count,
                            "Target Count": validation_count_views,
                            "Test Case": test_case
                        })
                    
                    elif validation_type == "Tags":
                        tags_source_count, tags_target_count = execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner)
                        test_case = "SUCCESS" if tags_source_count == tags_target_count else "FAILURE"
                        results_for_csv.append({
                            "Environment": env,
                            "Database": selected_database,
                            "Schema": selected_schema,
                            "Validation": "Tags",
                            "Source Count": tags_source_count,
                            "Target Count": tags_target_count,
                            "Test Case": test_case
                        })

                progress_bar.empty()
                
                # Display results
                results_df = pd.DataFrame(results_for_csv)
                
                st.markdown('<h3 class="sub-header">📈 Validation Results</h3>', unsafe_allow_html=True)
                display_summary_metrics(results_df)
                
                st.markdown("### 📋 Detailed Results")
                styled_df = style_dataframe(results_df)
                st.dataframe(styled_df, use_container_width=True)
                
                # Download button
                csv_bytes = results_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Validation Results",
                    data=csv_bytes,
                    file_name=f"masking_validation_results_{timestamp}.csv",
                    mime="text/csv"
                )

elif page == "🔐 Encryption DQ":
    st.markdown('<h1 class="main-header">🔐 Encryption Quality</h1>', unsafe_allow_html=True)
    
    # Encryption DQ functions
    @st.cache_data(ttl=300)
    def get_encryption_databases(env_prefix):
        """Get databases for encryption validation"""
        db_prefix = f"{env_prefix}_"
        db_query = f"""
            SELECT DATABASE_NAME 
            FROM INFORMATION_SCHEMA.DATABASES 
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
            AND DATABASE_NAME NOT LIKE '%_ENCRYPT'
            AND DATABASE_NAME NOT LIKE '%_MASKED%'
        """
        try:
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_encryption_schemas(database):
        """Get schemas for encryption validation"""
        schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
        try:
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    @st.cache_data(ttl=300)
    def get_encryption_classification_owners(env):
        """Get classification owners for encryption validation"""
        owner_query = f"""
            SELECT DISTINCT CLASSIFICATION_OWNER
            FROM {env}_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
        """
        try:
            rows = session.sql(owner_query).collect()
            return [row[0] for row in rows]
        except:
            return []

    def get_tables_and_columns_from_classification(env, selected_database, selected_schema, classification_owner):
        """Get tables and columns from classification details based on environment mapping"""
        try:
            # Convert selected database to PROD for classification lookup
            prod_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
            
            classification_query = f"""
                SELECT DISTINCT 
                    "TABLE" as table_name,
                    "COLUMN" as column_name
                FROM {env}_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                WHERE "DATABASE" = '{prod_database}'
                AND "SCHEMA" = '{selected_schema}'
                AND CLASSIFICATION_OWNER = '{classification_owner}'
                ORDER BY "TABLE", "COLUMN"
            """
            
            rows = session.sql(classification_query).collect()
            return [(row['TABLE_NAME'], row['COLUMN_NAME']) for row in rows]
        except Exception as e:
            st.error(f"Error fetching classification data: {e}")
            return []

    def check_table_column_exists_in_actual(database, schema, table_name, column_name):
        """Check if table and column exist in actual database"""
        try:
            check_query = f"""
                SELECT COUNT(*) as count
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table_name}'
                AND COLUMN_NAME = '{column_name}'
            """
            result = session.sql(check_query).collect()
            return result[0]['COUNT'] > 0 if result else False
        except:
            return False

    def check_table_column_exists_in_encrypted(encrypt_database, schema, table_name, column_name):
        """Check if table and column exist in encrypted database"""
        try:
            check_query = f"""
                SELECT COUNT(*) as count
                FROM {encrypt_database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table_name}'
                AND COLUMN_NAME = '{column_name}'
            """
            result = session.sql(check_query).collect()
            return result[0]['COUNT'] > 0 if result else False
        except:
            return False

    def run_encryption_validation(env, selected_database, selected_schema, classification_owner):
        """Run encryption validation comparing actual vs encrypted data"""
        with st.spinner("🔄 Running encryption validation..."):
            # Get encrypted database name
            encrypt_database = f"{selected_database}_ENCRYPT"
            
            # Get tables and columns from classification
            classification_data = get_tables_and_columns_from_classification(env, selected_database, selected_schema, classification_owner)
            
            if not classification_data:
                st.warning("⚠️ No classification data found for the selected criteria.")
                return pd.DataFrame([])

            results = []
            progress_bar = st.progress(0)
            
            for idx, (table_name, column_name) in enumerate(classification_data):
                progress_bar.progress((idx + 1) / len(classification_data))
                
                # Check if table and column exist in actual database
                actual_exists = check_table_column_exists_in_actual(selected_database, selected_schema, table_name, column_name)
                
                # Check if table and column exist in encrypted database
                encrypted_exists = check_table_column_exists_in_encrypted(encrypt_database, selected_schema, table_name, column_name)
                
                # Determine test case result
                if actual_exists and encrypted_exists:
                    test_case = "SUCCESS"
                    details = "Column exists in both actual and encrypted databases"
                elif actual_exists and not encrypted_exists:
                    test_case = "FAILURE"
                    details = "Column exists in actual but missing in encrypted database"
                elif not actual_exists and encrypted_exists:
                    test_case = "FAILURE"
                    details = "Column exists in encrypted but missing in actual database"
                else:
                    test_case = "FAILURE"
                    details = "Column missing in both actual and encrypted databases"
                
                results.append({
                    "Environment": env,
                    "Database": selected_database,
                    "Encrypted Database": encrypt_database,
                    "Schema": selected_schema,
                    "Table": table_name,
                    "Column": column_name,
                    "Classification Owner": classification_owner,
                    "Actual DB Exists": "Yes" if actual_exists else "No",
                    "Encrypted DB Exists": "Yes" if encrypted_exists else "No",
                    "Test Case": test_case,
                    "Details": details
                })

            progress_bar.empty()
            return pd.DataFrame(results)

    def run_encryption_table_level_validation(env, selected_database, selected_schema, classification_owner):
        """Run table-level encryption validation"""
        with st.spinner("🔄 Running table-level encryption validation..."):
            encrypt_database = f"{selected_database}_ENCRYPT"
            
            # Get unique tables from classification
            classification_data = get_tables_and_columns_from_classification(env, selected_database, selected_schema, classification_owner)
            unique_tables = list(set([table for table, _ in classification_data]))
            
            if not unique_tables:
                st.warning("⚠️ No tables found in classification data.")
                return pd.DataFrame([])

            results = []
            progress_bar = st.progress(0)
            
            for idx, table_name in enumerate(unique_tables):
                progress_bar.progress((idx + 1) / len(unique_tables))
                
                # Check if table exists in actual database
                try:
                    actual_table_query = f"""
                        SELECT COUNT(*) as count
                        FROM {selected_database}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{selected_schema}'
                        AND TABLE_NAME = '{table_name}'
                    """
                    actual_result = session.sql(actual_table_query).collect()
                    actual_table_exists = actual_result[0]['COUNT'] > 0 if actual_result else False
                except:
                    actual_table_exists = False
                
                # Check if table exists in encrypted database
                try:
                    encrypted_table_query = f"""
                        SELECT COUNT(*) as count
                        FROM {encrypt_database}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{selected_schema}'
                        AND TABLE_NAME = '{table_name}'
                    """
                    encrypted_result = session.sql(encrypted_table_query).collect()
                    encrypted_table_exists = encrypted_result[0]['COUNT'] > 0 if encrypted_result else False
                except:
                    encrypted_table_exists = False
                
                # Determine test case result
                if actual_table_exists and encrypted_table_exists:
                    test_case = "SUCCESS"
                    details = "Table exists in both actual and encrypted databases"
                elif actual_table_exists and not encrypted_table_exists:
                    test_case = "FAILURE"
                    details = "Table exists in actual but missing in encrypted database"
                elif not actual_table_exists and encrypted_table_exists:
                    test_case = "FAILURE"
                    details = "Table exists in encrypted but missing in actual database"
                else:
                    test_case = "FAILURE"
                    details = "Table missing in both actual and encrypted databases"
                
                results.append({
                    "Environment": env,
                    "Database": selected_database,
                    "Encrypted Database": encrypt_database,
                    "Schema": selected_schema,
                    "Table": table_name,
                    "Classification Owner": classification_owner,
                    "Actual DB Table Exists": "Yes" if actual_table_exists else "No",
                    "Encrypted DB Table Exists": "Yes" if encrypted_table_exists else "No",
                    "Test Case": test_case,
                    "Details": details
                })

            progress_bar.empty()
            return pd.DataFrame(results)

    # UI Controls
    st.markdown('<h3 class="sub-header">🎛️ Control Panel</h3>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        encrypt_env = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"], key="encrypt_env")
    with col2:
        encrypt_database_list = get_encryption_databases(encrypt_env)
        encrypt_selected_database = st.selectbox("🏢 Database", encrypt_database_list, key="encrypt_db_select")
    with col3:
        encrypt_schema_list = get_encryption_schemas(encrypt_selected_database) if encrypt_selected_database else []
        encrypt_selected_schema = st.selectbox("📁 Schema", encrypt_schema_list, key="encrypt_schema_select")
    with col4:
        encrypt_classification_owners = get_encryption_classification_owners(encrypt_env)
        encrypt_classification_owner = st.selectbox("👤 Classification Owner", encrypt_classification_owners, key="encrypt_owner_select")

    # Validation type selection
    st.markdown('<h3 class="sub-header">🔍 Validation Type</h3>', unsafe_allow_html=True)
    validation_type = st.radio(
        "Select validation level:",
        ["🔍 Column Level Validation", "📊 Table Level Validation"],
        horizontal=True
    )

    if st.button("🚀 Run Encryption Validation", type="primary", key="encrypt_validate"):
        if not all([encrypt_env, encrypt_selected_database, encrypt_selected_schema, encrypt_classification_owner]):
            st.error("❌ Please fill in all required fields")
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if validation_type == "🔍 Column Level Validation":
                df = run_encryption_validation(encrypt_env, encrypt_selected_database, encrypt_selected_schema, encrypt_classification_owner)
                validation_name = "column_encryption"
            else:
                df = run_encryption_table_level_validation(encrypt_env, encrypt_selected_database, encrypt_selected_schema, encrypt_classification_owner)
                validation_name = "table_encryption"
            
            if not df.empty:
                st.markdown('<h3 class="sub-header">📈 Encryption Validation Results</h3>', unsafe_allow_html=True)
                display_summary_metrics(df)
                
                st.markdown("### 📋 Detailed Results")
                styled_df = style_dataframe(df)
                st.dataframe(styled_df, use_container_width=True)
                
                # Download button
                csv_bytes = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Encryption Results",
                    data=csv_bytes,
                    file_name=f"{validation_name}_validation_{timestamp}.csv",
                    mime="text/csv"
                )
            else:
                st.info("ℹ️ No data found for the selected criteria.")
    
    # Information box
    st.markdown("""
    <div class="info-box">
        <h3>📋 Encryption Validation Information</h3>
        <p><strong>Column Level Validation:</strong> Compares each column from classification details between actual and encrypted databases.</p>
        <p><strong>Table Level Validation:</strong> Compares table existence between actual and encrypted databases.</p>
        <p><strong>Database Mapping:</strong> Encrypted database name is formed by adding '_ENCRYPT' suffix to the selected database.</p>
        <p><strong>Classification Mapping:</strong> Classification details are always stored with PROD database names and mapped to the selected environment.</p>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d; padding: 2rem;'>
    <p>🎯 ZDQ Hub - Data Quality Excellence • Built with ❤️ using Streamlit</p>
</div>
""", unsafe_allow_html=True)