# 🎯 ZDQ Hub - Data Quality Excellence

A modern, beautiful Streamlit application for automated data quality validation across your data pipeline. Built specifically for Snowflake environments with comprehensive validation capabilities.

## ✨ Features

### 🏠 Home Dashboard
- **Beautiful Landing Page**: Modern gradient design with intuitive navigation
- **Feature Overview**: Clear description of all validation modules
- **Quick Navigation**: Easy access to all validation processes

### 📊 Data Ingestion DQ
- **Count Validation**: Compare source vs target row counts
- **Data Validation**: Comprehensive data integrity checks  
- **Duplicate Detection**: Identify and report duplicate records
- **Progress Tracking**: Real-time progress bars for long-running validations
- **Summary Metrics**: Visual cards showing test results at a glance

### 🎭 Masking DQ
- **MD Tables Validation**: Verify masking table metadata
- **MD Columns Validation**: Check column-level masking configuration
- **Data Set Validation**: Validate data set consistency
- **Views Validation**: Ensure view creation for masked data
- **Tags Validation**: Verify data classification tags

### 🔐 Encryption DQ (Coming Soon)
- **Encryption Status**: Monitor encryption compliance
- **Key Management**: Validate encryption key usage
- **Security Compliance**: Comprehensive security checks

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Snowflake account with appropriate permissions
- Snowpark session configured

### Installation

1. **Clone or download the application files**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up your Snowflake connection** (ensure your Snowpark session is configured)

4. **Run the application:**
   ```bash
   streamlit run app.py
   ```

5. **Access the app** at `http://localhost:8501`

## 🎨 What's New & Improved

### 🌈 Visual Enhancements
- **Modern Gradient Design**: Beautiful color schemes throughout
- **Responsive Layout**: Optimized for all screen sizes
- **Interactive Cards**: Hover effects and animations
- **Professional Typography**: Clean, readable fonts with proper hierarchy

### 📊 Enhanced User Experience
- **Progress Indicators**: Real-time progress bars for all validations
- **Loading Spinners**: Clear feedback during operations
- **Error Handling**: Graceful error messages with helpful icons
- **Success/Failure Styling**: Color-coded results with gradients

### ⚡ Performance Improvements
- **Caching**: Optimized data fetching with TTL caching
- **Session Management**: Better Snowflake session handling
- **Resource Optimization**: Efficient memory usage

### 📈 Better Results Display
- **Summary Metrics**: Visual cards showing totals, pass/fail counts, and success rates
- **Styled DataFrames**: Beautiful table styling with gradient highlights
- **Export Features**: Timestamped CSV downloads for all results

## 🎛️ How to Use

### Data Ingestion DQ
1. **Select Environment**: Choose DEV, QA, UAT, or PROD
2. **Choose Validation Rule**: Pick from Count, Data, or Duplicate validation
3. **Configure Parameters**: Set database, schema, load type, and load group
4. **Run Validation**: Click the run button and watch progress
5. **Review Results**: See summary metrics and detailed results
6. **Export Data**: Download results as CSV

### Masking DQ  
1. **Select Environment**: Choose your target environment
2. **Choose Database & Schema**: Select from available options
3. **Pick Classification Owner**: Select the data owner
4. **Run All Validations**: Execute comprehensive masking checks
5. **Review Results**: See all validation results in one view
6. **Export Report**: Download complete validation report

## 🔧 Configuration

### Environment Database Mapping
The app uses these database mappings:
- **DEV**: `dev_db_manager`
- **QA**: `qa_db_manager`  
- **UAT**: `uat_db_manager`
- **PROD**: `prod_db_manager`

### Data Lake Mapping
- **DEV**: `dev_datalake`
- **QA**: `qa_datalake`
- **UAT**: `uat_datalake` 
- **PROD**: `prod_datalake`

## 🎨 Color Scheme

The app uses a modern color palette:
- **Primary**: Blue gradients (#667eea to #764ba2)
- **Success**: Green gradients (#11998e to #38ef7d)
- **Warning**: Orange gradients (#fc466b to #3f5efb)
- **Info**: Blue gradients (#74b9ff to #0984e3)

## 📊 Features Overview

### ✅ Enhanced Styling
- Gradient backgrounds and cards
- Smooth hover animations
- Professional color scheme
- Responsive design

### 🔄 Better Functionality  
- Real-time progress tracking
- Improved error handling
- Optimized performance with caching
- Better session management

### 📈 Rich Analytics
- Summary metrics with visual cards
- Success/failure rates
- Detailed result tables
- Exportable reports

## 🤝 Support

For issues or questions about the ZDQ Hub:
1. Check the error messages for specific guidance
2. Verify your Snowflake connection and permissions
3. Ensure all required databases and schemas exist
4. Contact your data team for database-specific issues

## 🛠️ Technical Details

- **Framework**: Streamlit 1.28+
- **Database**: Snowflake with Snowpark
- **Styling**: Custom CSS with gradients and animations
- **Charts**: Plotly for potential future visualizations
- **Data Processing**: Pandas for result manipulation

---

🎯 **ZDQ Hub - Where Data Quality Meets Excellence** • Built with ❤️ using Streamlit