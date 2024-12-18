

---
# Metro Mavericks: TriMet GPS Data Analysis

**Metro Mavericks** is a comprehensive project designed to analyze GPS data from **TriMet**, the public transportation system in Portland, Oregon. The project leverages real-time data to provide actionable insights into transit operations, traffic patterns, and route efficiencies. It combines robust data engineering, visualization, and analysis techniques to improve urban mobility.

---
## Team Members

- **Sriram Nurani Subramanyam** 
- **Mahesh Vankayalapati** 
- **Sai Krishna Mukund**
- **Nitin Sarangadhar**
--- 

## Table of Contents

1. [Introduction](#introduction)
2. [Objectives](#objectives)
3. [Features](#features)
4. [Data Flow and Architecture](#data-flow-and-architecture)
5. [Technologies Used](#technologies-used)
6. [Challenges and Solutions](#challenges-and-solutions)
7. [Insights and Visualizations](#insights-and-visualizations)
8. [Setup and Installation](#setup-and-installation)
9. [Future Enhancements](#future-enhancements)
10. [Acknowledgements](#acknowledgements)

---

## Introduction

**Metro Mavericks** aims to process, validate, and analyze real-time GPS data from TriMet transit vehicles. By leveraging tools like **Google Maps API**, **Mapbox GL**, and **Google Cloud**, the project visualizes transit patterns and uncovers inefficiencies to improve service quality and reliability.

---

## Objectives

1. Process and validate GPS data from TriMet APIs.
2. Analyze transit metrics such as speed, route efficiency, and trip patterns.
3. Visualize data interactively using mapping tools for better insights.
4. Archive large datasets securely for long-term use.

---

## Features

### 1. Data Gathering
- Collects real-time GPS data from TriMet APIs using Python.
- Implements error handling with retries and exponential backoff.
- Stores raw data in JSON format for pre-processing.

### 2. Data Validation
- Validates GPS coordinates and timestamps to ensure accuracy.
- Handles missing or erroneous data points.

### 3. Data Transformation
- Transforms raw data into actionable metrics, such as:
  - Distance traveled
  - Average and real-time speed
  - Stop delays
- Standardizes data formats for further analysis.

### 4. Data Storage
- Archives data securely in **Google Cloud Storage**.
- Stores processed data in **PostgreSQL** for fast querying and analysis.

### 5. Data Visualization
- Uses **Google Maps API** and **Mapbox GL** for interactive maps.
- Visualizes traffic patterns, route efficiencies, and vehicle delays.

### 6. Security and Maintenance
- Compresses and encrypts data using **AES** and **RSA** for secure storage.
- Implements automated archival and batch processing for scalability.

---

## Data Flow and Architecture

```plaintext
TriMet API → Python Data Collection → JSON Storage → Data Validation/Transformation 
→ PostgreSQL Database → Visualization (Mapbox GL, Google Maps API) → Insights
```

---

## Technologies Used

- **Programming Languages**: Python, SQL.
- **Data Engineering**: PostgreSQL, Google Cloud Pub/Sub.
- **Visualization**: Mapbox GL, Google Maps API.
- **Security**: AES, RSA encryption.
- **Infrastructure**: Google Cloud Storage, Google Cloud Platform.

---

## Challenges and Solutions

### 1. High-Frequency Data Streams
- **Challenge**: Handling 250,000–350,000 GPS points daily.
- **Solution**: Batched processing and efficient data storage mechanisms.

### 2. Data Inconsistencies
- **Challenge**: GPS data often contains gaps or errors.
- **Solution**: Implemented rigorous validation and error handling during ingestion.

### 3. Network Latency
- **Challenge**: API timeouts during data collection.
- **Solution**: Used retries with exponential backoff to mitigate delays.

---

## Insights and Visualizations

### **Tuesday Outbound Trips (9–11 AM)**
- **Objective**: Analyze delays and speed variations during morning commutes.
- **Insights**:
  - Identified bottlenecks in specific routes.
  - Adjusted transit schedules to minimize delays.

### **PSU Sunday Trips (9–11 AM)**
- **Objective**: Evaluate weekend traffic to and from Portland State University.
- **Insights**:
  - Reduced stop time at underutilized stations.
  - Improved route planning for campus access.

---

## Setup and Installation

### Prerequisites
1. Python 3.9 or above.
2. PostgreSQL 12 or above.
3. Google Cloud SDK (for deploying storage and APIs).

### Steps
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/<your-username>/metro-mavericks.git
   cd metro-mavericks
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up the Database**:
   - Initialize PostgreSQL and create the required tables:
     ```sql
     CREATE DATABASE metro_mavericks;
     ```

4. **Run the Application**:
   ```bash
   python main.py
   ```

5. **Visualize Data**:
   - Use the web interface or tools like Mapbox GL for interactive visualizations.

---

## Future Enhancements

1. Integrate **Machine Learning** models to predict delays and optimize routes.
2. Support real-time notifications for transit delays.
3. Deploy a public-facing web interface for broader accessibility.

---

## Acknowledgements

Special thanks to:
- **TriMet** for providing real-world datasets.
- **Dr. Bruce Irvin** and **Mina Vu** for their invaluable guidance.



