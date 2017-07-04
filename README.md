# Anomaly Coding Challenge
This is an implementation of the [Insight Data Engineering Anomaly Detection Coding Challenge]( https://github.com/InsightDataScience/anomaly_detection ) using python3.6.

## Libraries used
The implmentation makes use of ```simplejson``` library in addition to the standard python libraries

## Running the code
Simply run the ```run.sh``` file.
```
./run.sh
```
This bash script runs the python script with the arguments being

``` python <script path> <batch_log filepath> <stream_log filepath> <output filepath>```

The default values currently used in the script are 

```python ./src/anomaly_detection.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json ```
