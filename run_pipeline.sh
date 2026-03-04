#!/bin/bash
cd /home/pokr/projects/etl
source venv/bin/activate
python src/load_pipeline.py >> pipeline.log 2>&1
