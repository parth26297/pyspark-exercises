# Requirements
1. Java (8)
2. Spark (3.2.0) & Hadoop (2.7)
3. Python (3.x)

# Setup
1. Update `pyspark` version in *requirements.txt* file, to the installed Spark version on your system
2. Update `delta-spark` version in *requirements.txt* file, that is compatible with your Spark version [Check compatibility here](https://docs.delta.io/latest/releases.html)
3. Create python virtual environment with `python -m venv .venv`
4. Install required libraries with `pip install -r requirements.txt`
5. Download the data from [here](https://drive.google.com/file/d/1mwHptnNjspfY7MpPtxLuvTFwL29p0eGE/view?usp=sharing) and put the data folder in the root folder of this exercise.

# Notes
- Delta tables in these exercises are not accessible simultaneously for 2 separate spark sessions. Make sure to keep only one spark session running at a time.