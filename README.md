# 🌤️ Real-time Weather Data Pipeline

A data engineering project that continuously fetches weather data from OpenWeatherMap API, processes it, stores it in PostgreSQL, and visualizes trends through an interactive dashboard.

## 🎯 Project Overview

This pipeline demonstrates key data engineering skills:
- **Data Ingestion**: Fetching data from REST APIs
- **Data Transformation**: Cleaning and structuring raw data
- **Data Storage**: Designing and populating a database
- **Automation**: Scheduling recurring data collection
- **Visualization**: Building interactive dashboards

## 🛠️ Technologies Used

- **Python 3.8+**: Main programming language
- **OpenWeatherMap API**: Weather data source
- **PostgreSQL**: Database for storing processed data
- **Pandas**: Data manipulation and analysis
- **Streamlit**: Interactive dashboard
- **Plotly**: Data visualization
- **Schedule**: Task automation

## 📋 Prerequisites

- Python 3.8 or higher
- PostgreSQL installed locally
- OpenWeatherMap API key (free tier)

## 🚀 Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/weather-data-pipeline.git
cd weather-data-pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:
```bash
OPENWEATHER_API_KEY=your_api_key_here
DEFAULT_CITY=Toronto
```

### 5. Test API connection
```bash
python src/test_api.py
```

## 📁 Project Structure
```
weather-data-pipeline/
├── src/
│   ├── config.py         # Configuration loader
│   ├── test_api.py       # API connection tester
│   ├── ingestion.py      # Data fetching
│   ├── transformation.py # Data processing
│   ├── storage.py        # Database operations
│   └── dashboard.py      # Streamlit dashboard
├── sql/
│   └── schema.sql        # Database schema
├── tests/
│   └── test_pipeline.py  # Unit tests
├── .env                  # Environment variables (not in git)
├── .gitignore
├── requirements.txt
└── README.md
```

## 🎯 Features

- ✅ Real-time weather data collection
- ✅ Multi-city tracking
- ✅ Automated data pipeline
- ✅ Data quality validation
- ✅ Interactive dashboard
- ✅ Historical trend analysis

## 📊 Dashboard

Run the dashboard:
```bash
streamlit run src/dashboard.py
```

## 🤝 Contributing

Contributions welcome! Please feel free to submit a Pull Request.

## 📝 License

MIT License

## 👤 Author

**Your Name**
- GitHub: [@Anshpreetlayal](https://github.com/Anshpreetlayal)
- LinkedIn: [Anshpreetlayal](https://linkedin.com/in/Anshpreetlayal)