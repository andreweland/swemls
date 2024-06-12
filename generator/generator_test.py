#!/usr/bin/env python3

import csv
import sys
import re
import tempfile
import shutil
import subprocess

NHS_F3_THRESHOLD = 0.7
CREATININE_UPPER_LIMIT = 1000

def run_integration_test(work_directory):
    r = subprocess.run(["./generator/nhs.py", "--test"])
    if r.returncode != 0:
        print("nhs: self test failed")
        return False
    r = subprocess.run(["./generator/generator.py", "--days=25", f"--output={work_directory}"])
    if r.returncode != 0:
        print("generator: failed")
        return False
    training_csv = f"{work_directory}/training.csv"
    with open(training_csv) as f:
        r = csv.reader(f)
        headers = next(r)
        columns = [i for (i, header) in enumerate(headers) if header.startswith("creatinine_result_")]
        for row in r:
            for column in columns:
                if len(row[column]) > 0 and float(row[column]) > CREATININE_UPPER_LIMIT:
                    print(f"generator: bad creatinine value: {float(row[column])}")
                    return False
    aki_predictions_csv = f"{work_directory}/aki_predictions.csv"
    r = subprocess.run(["./generator/nhs.py", f"--input={training_csv}", f"--output={aki_predictions_csv}"])
    if r.returncode != 0:
        print("nhs: prediction failed")
        return False
    r = subprocess.run(["./generator/score.py", f"--expected={training_csv}", f"--predictions={aki_predictions_csv}"], capture_output=True)
    if r.returncode != 0:
        print("score: prediction failed")
        return False
    print(r.stdout.decode("utf-8"))
    f3_pattern = re.compile("model: f3: ([0-9.]+) ")
    match = f3_pattern.search(str(r.stdout))
    if match is None:
        print("score: failed to generate expected output")
        return False
    f3_score = float(match.group(1))
    if f3_score < NHS_F3_THRESHOLD:
        print(f"score: f3 score {f3_score} below expected threshold of {NHS_F3_THRESHOLD}")
        return False
    return True

def main():
    work_directory = tempfile.mkdtemp()
    ok = False
    try:
        ok = run_integration_test(work_directory)
        if ok:
            print("integration test: passed")
        else:
            print("integration test: failed")
    finally:
        shutil.rmtree(work_directory)
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()