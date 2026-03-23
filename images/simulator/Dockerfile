### 1. Get Linux
FROM python:3.11

RUN apt-get update && apt-get install -y \
    iproute2 \
 && rm -rf /var/lib/apt/lists/*

### 2. Install python-can and cantools
COPY images/simulator/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

### 3. Copy can server configuration
COPY images/simulator/simulation.dbc /simulation.dbc

### 4. Start server  
ADD images/simulator/simulator.py /
CMD ["python3", "simulator.py"]
