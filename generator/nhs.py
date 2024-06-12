#!/usr/bin/env python3

import argparse
import csv
import datetime
import statistics
import sys
import unittest

SEX_MALE = 0
SEX_FEMALE = 1

def parse_creatinine_results(row):
    results = []
    for i in range(0, len(row), 2):
        if row[i] == "":
            break
        date = datetime.datetime.strptime(row[i], "%Y-%m-%d %H:%M:%S")
        result = float(row[i + 1])
        results.append((date, result))
    return results

# Creatinine reference ranges
# From https://www.nbt.nhs.uk/sites/default/files/document/Biochemistry%20Age%20Related%20Reference%20Ranges.pdf
CREATININE_REFERENCE_RANGES = {
    SEX_MALE: [
        ((0, 1), 14.0, 34.0),
        ((1, 3), 15.0, 31.0),
        ((3, 5), 23.0, 37.0),
        ((5, 7), 25.0, 42.0),
        ((7, 9), 30.0, 47.0),
        ((9, 11), 29.0, 56.0),
        ((11, 12), 36.0, 64.0),
        ((12, 13), 36.0, 67.0),
        ((13, 14), 38.0, 76.0),
        ((14, 15), 40.0, 83.0),
        ((15, 16), 47.0, 98.0),
        ((16, 17), 54.0, 99.0),
        ((17, 100), 59.0, 104.0),
    ],
    SEX_FEMALE: [
        ((0, 1), 14.0, 34.0),
        ((1, 3), 15.0, 31.0),
        ((3, 5), 23.0, 37.0),
        ((5, 7), 25.0, 42.0),
        ((7, 9), 30.0, 47.0),
        ((9, 11), 29.0, 56.0),
        ((11, 12), 36.0, 64.0),
        ((12, 13), 36.0, 67.0),
        ((13, 14), 38.0, 74.0),
        ((14, 15), 43.0, 75.0),
        ((15, 16), 44.0, 79.0),
        ((16, 17), 48.0, 81.0),
        ((17, 100), 45.0, 84.0),
    ]
}

# AKI algorithm
# https://www.england.nhs.uk/wp-content/uploads/2014/06/psa-aki-alg.pdf

def has_aki(age, sex, test_results, now):
    c1 = test_results[-1][1]
    if len(test_results) == 1:
        return False
    ulri = 0.0
    for ((age_begin, age_end), _, upper_reference) in CREATININE_REFERENCE_RANGES[sex]:
        if age >= age_begin and age < age_end:
            ulri = upper_reference
            break
    else:
        raise Exception("No valid creatinine reference range")
    d = test_results[-1][0] - test_results[-2][0]
    rv = 0.0
    if d < datetime.timedelta(days=365):
        within_7 = [result for (date, result) in test_results[:-1] if now - date < datetime.timedelta(days=7)]
        within_365 = [result for (date, result) in test_results[:-1] if now - date > datetime.timedelta(days=7) and now - date < datetime.timedelta(days=365)]
        within_7_rv = 0.0
        if len(within_7) > 0:
            rv1 = min(within_7)
            within_7_rv = c1 / rv1
        within_365_rv = 0.0
        if len(within_365) > 0:
            rv2 = statistics.median(within_365)
            within_365_rv = c1 / rv2
        rv = max([within_7_rv, within_365_rv])
        if rv > 1.5:
            if age < 18:
                if c1 > 3.0 * ulri:
                    return True # AKI 3
            elif c1 > 354.0:
                return True # AKI 3
            if rv >= 3.0:
                return True # AKI 3
            elif rv >= 2.0:
                return True # AKI 2
    return False

class AKITest(unittest.TestCase):

    def test_recent_test_result_with_aki(self):
        now = datetime.datetime(year=2023, month=1, day=9)
        self.assertTrue(has_aki(42, SEX_MALE, [(now - datetime.timedelta(days=1), 104.0), (now, 300.0)], now))

    def test_recent_test_result_without_aki(self):
        now = datetime.datetime(year=2023, month=1, day=9)
        self.assertFalse(has_aki(42, SEX_MALE, [(now - datetime.timedelta(days=1), 104.0), (now, 102.0)], now))

    def test_old_test_result_with_aki(self):
        now = datetime.datetime(year=2023, month=1, day=9)
        self.assertTrue(has_aki(42, SEX_MALE, [(now - datetime.timedelta(days=30), 104.0), (now, 300.0)], now))

    def test_old_test_result_without_aki(self):
        now = datetime.datetime(year=2023, month=1, day=9)
        self.assertTrue(has_aki(42, SEX_MALE, [(now - datetime.timedelta(days=30), 104.0), (now, 300.0)], now))

    def test_old_test_result_lower_than_recent_with_aki(self):
        now = datetime.datetime(year=2023, month=1, day=9)
        old1 = (now - datetime.timedelta(days=40), 44.0)
        old2 = (now - datetime.timedelta(days=30), 46.0)
        recent1 = (now - datetime.timedelta(days=4), 122.0)
        recent2 = (now - datetime.timedelta(days=3), 120.0)
        self.assertTrue(has_aki(42, SEX_MALE, [old1, old2, recent1, recent2], now))

    def test_old_test_result_higher_than_recent_with_aki(self):
        now = datetime.datetime(year=2023, month=1, day=9)
        old1 = (now - datetime.timedelta(days=40), 122.0)
        old2 = (now - datetime.timedelta(days=30), 120.0)
        recent1 = (now - datetime.timedelta(days=4), 44.0)
        recent2 = (now - datetime.timedelta(days=3), 121.0)
        self.assertTrue(has_aki(42, SEX_MALE, [old1, old2, recent1, recent2], now))

def test():
    suite = unittest.TestSuite()
    for method in dir(AKITest):
        if method.startswith("test_"):
            suite.addTest(AKITest(method))
    runner = unittest.TextTestRunner()
    return runner.run(suite).wasSuccessful()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", default=False, action="store_true")
    parser.add_argument("--input", default="training.csv", action="store")
    parser.add_argument("--output", default="nhs_aki.csv", action="store")
    flags = parser.parse_args()
    if flags.test:
        sys.exit(0 if test() else 1)
    r = csv.reader(open(flags.input))
    headers = next(r)
    age_column = headers.index("age")
    sex_column = headers.index("sex")
    creatinine_begin_column = headers.index("creatinine_date_0")
    w = csv.writer(open(flags.output, "w"))
    w.writerow(("aki",))
    for row in r:
        age = int(row[age_column])
        sex = SEX_MALE if row[sex_column] == "m" else SEX_FEMALE
        results = parse_creatinine_results(row[creatinine_begin_column:])
        predicted = has_aki(age, sex, results, results[-1][0])
        w.writerow(("y" if predicted else "n",))

if __name__ == "__main__":
    main()