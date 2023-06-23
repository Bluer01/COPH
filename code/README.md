## mondu.py

This file is the MongoDB upload tool. It is written in Python and can be executed in the command line to upload a JSON or CSV file of user measurements to the database. It follows the following flow:

1. Determine the path of the file
2. Connect to the MongoDB database
3. Read and parse the file
4. Either print the samples if debugging, or upload the samples otherwise

### Parsing the file

Parsing the file is as simple as:

1. Determine the format of the file (CSV or JSON)
2. Read the file
3. Add each row to a list of python dictionaries
4. Return data

### Preparing the samples

Both printing and uploading the samples feature the first step of preparing the samples. This is performed as follows:

1. Conditional preprocessing of samples with nonstandard formatting (such as prescriptions)
2. For each record in the data, create or append to a dictionary of the sample, measurement, the context and additional information
3. Return the samples to then update the MongoDB collection of measurements or to print the samples
