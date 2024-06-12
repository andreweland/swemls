#!/usr/bin/env python3

import argparse
import csv
import random

def precision(true_positives, false_positives):
    return true_positives / (true_positives + false_positives)

def recall(true_positives, false_negatives):
    return true_positives / (true_positives + false_negatives)

def fb_score(true_positives, false_positives, false_negatives, b):
    p = precision(true_positives, false_positives)
    r = recall(true_positives, false_negatives)
    return (1 + (b*b)) * ((p * r) / (((b * b) * p) + r))

def score(results):
    true_positives = 0
    true_negatives = 0
    false_positives = 0
    false_negatives = 0
    for (prediction, expected) in results:
        if prediction == expected:
            if prediction:
                true_positives += 1
            else:
                true_negatives += 1
        else:
            if prediction:
                false_positives += 1
            else:
                false_negatives += 1
    f3 = fb_score(true_positives, false_positives, false_negatives, 3)
    return f"f3: {f3:.5f} tp: {true_positives} tn: {true_negatives} fp: {false_positives} fn: {false_negatives}"

def read_aki(filename):
    r = csv.reader(open(filename))
    headers = next(r)
    aki_column = headers.index("aki")
    next(r) # skip headers
    return [row[aki_column] == "y" or row[aki_column]== "1" for row in r]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", default="coursework1-test.expected.csv", action="store")
    parser.add_argument("--predictions", default="aki.csv", action="store")

    flags = parser.parse_args()
    predictions = read_aki(flags.predictions)
    expected = read_aki(flags.expected)

    model = zip(predictions, expected)
    perfect = zip(expected, expected)
    arbitrary = [(random.choice([True, False]), e) for e in expected]

    print(f"model: {score(model)}")
    print(f"arbitrary: {score(arbitrary)}")
    print(f"perfect: {score(perfect)}")

if __name__ == "__main__":
    main()