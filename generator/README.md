# Software Engineering for Machine Learning Systems

## Generator

This directory contains code to generate synthetic creatinine blood test results, acute kidney injury events, and patient admit and discharge within a hospital. It makes no attempt to be clinically accurate, but believable enough to reason about when building a system.

A random set of patients is selected to suffer acute kidney injury, and the blood test results for these patients are constructed such that the [NHS reference AKI algorithm](https://www.england.nhs.uk/akiprogramme/aki-algorithm/) performs well. Bias is deliberately added to enable the design of simple machine learning models with better performance.


Patient ages and sexes are reasonably distributed, as are their blood result values. Event times and frequencies bear some resemblance to what you'd see in a reasonably sized hospital.

The generator can be run with:

```
pip3 install -r ../requirements.txt
./generator.py --data=../data --history=90 --days=30 --output=.
```
* `--data` the location of the census and name datasets used to generate the synthetic population.
* `--history` the number of days of patient history that should be generated, written as a CSV dataset of historical blood test results.
* `--days` the number of hospital events that should be generated, written as an MLLP encoding set of HL7 messages.
* `--output` the directory in which output files should be written.

The generator produces these outputs:
* `messages.mllp` an MLLP encoded sequence of HL7 message representing events at the hospital over the given number of days. It includes:
    * `ADT^A01` events when a patient is admitted. These messages include patient demographic data.
    * `ORU^R01` events when a blood test result is recorded. These messages do not include patient demographic data, requiring receivers to match these messages with the corresponding `ADT^A01`.
    * `ADT^A03` events when a patient is discharged

    The message sequence starts with `ADT^A01` messages for all currently admitted patients at the simulated start time, to make it easier for systems receiving messages to handle startup.
* `akis.csv` a CSV file detailing the patients experiencing an acute kidney injury, together the with the timestamp of the first blood test result following injury, and whether the injury is detected by the NHS algorithm. Note the file includes false positives from the NHS algorithm (ie patients for which the NHS algorithm incorrectly detects an injury).
* `history.csv` a CSV file with the historical blood test results for patients who may appear in `messages.mllp`. Systems receiving the generated HL7 messages will need to take the blood test results from this file into account to generate accurate predictions.
* `training.csv` a CSV file with a dataset that can be used to train a model to detect acute kidney injury. For each patient, the file includes demographics, blood test result values, and whether they were diagonsed with an acute kidney injury. If the `aki` column is `y` for a patient, then their most recent blood test result represents the point at which that diagonsis was made. No further blood test results are included post diagonsis.

  Note that the same event stream is used to create both this output file, and the others, so if `training.csv` is used, the other files shouldn't be; fresh data should be generated instead.