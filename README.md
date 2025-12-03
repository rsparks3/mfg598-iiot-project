# MFG598 IIoT Project - DMLS 3D Printer Monitoring

This project demonstrates an Industrial Internet of Things (IIoT) data pipeline for monitoring Direct Metal Laser Sintering (DMLS) 3D printers. The system implements a three-tier architecture that collects telemetry data from physical devices, processes it at the edge, stores it in the cloud, and visualizes it through an interactive dashboard.

![System Architecture](imgs/Screenshot%202025-12-02%20at%2011.45.13%20PM.png)

## Components

### Physical Device (`physical-device-test.py`)
Simulates a DMLS 3D printer sensor array that generates telemetry data. The physical device collects:
- **Temperature data**: 10,000 temperature readings (100×100 sensor array) ranging from 20°C to 80°C
- **Power consumption**: Machine power usage in kW
- **Vibration**: Vibration measurements in mm/s

The device transmits data via OPC UA protocol to the edge device, mimicking real industrial equipment communication standards.

### Edge Device (`edge-device.py`)
Serves as an intermediary OPC UA server that receives telemetry from physical devices and forwards processed data to the cloud. Key functions include:
- Buffers incoming telemetry data (configurable buffer size)
- Averages multiple data points to reduce cloud transmission overhead
- Calculates preliminary statistics (min, max, mean, standard deviation)
- Sends aggregated data to the cloud device via HTTP REST API

### Cloud Device (`cloud-device.py`)
A Flask-based REST API server that acts as the cloud data ingestion and storage layer. Features include:
- Receives telemetry data from edge devices via HTTP POST
- Stores data in a PostgreSQL database with full telemetry history
- Computes and stores temperature statistics for each record
- Provides REST endpoints for data retrieval filtered by machine ID or time range
- Supports querying machine lists and retrieving specific records

### Dashboard (`dashboard.py`)
An interactive Streamlit web application for real-time monitoring and data visualization. The dashboard provides:
- Machine selection and filtering capabilities
- Real-time telemetry data display with auto-refresh
- Temperature heatmap visualization (100×100 grid)
- Time-series plots for power consumption and vibration
- Statistical summaries (min, max, mean, standard deviation)
- Historical data browser with record-by-record navigation

## Technology Stack
- **OPC UA**: Industrial communication protocol for device-to-edge communication
- **Flask**: RESTful API framework for cloud services
- **PostgreSQL**: Relational database for telemetry data storage
- **Streamlit**: Interactive dashboard framework
- **Python asyncio**: Asynchronous programming for OPC UA operations
