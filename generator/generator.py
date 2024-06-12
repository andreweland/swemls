#!/usr/bin/env python3

import argparse
import csv
import datetime
import os
import queue
import random
import statistics

from hl7apy.core import Message
from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.parser import parse_message

import nhs
import population

# 800 beds, 20 admissions per day, implies an average stay of 40 days.
# Emperically we need a mean of ~30 admissions to stabalise around
# 800 admitted patients
HOSPITAL_BEDS = 800
STAY_DAYS_DISTRIBUTION = statistics.NormalDist(HOSPITAL_BEDS/40, 35)
MIN_MRN = 1000000 * 100
MAX_MRN = 2000000 * 100

HL7_HOSPITAL_NAME = "SOUTH RIVERSIDE"
HL7_APP = "SIMULATION"

EVENT_DAY_START = "day_start"
EVENT_ADMIT = "admit"
EVENT_DISCHARGE = "discharge"
EVENT_BLOOD_TEST = "blood_test"
EVENT_BLOOD_TEST_AKI = "blood_test_aki"

class HospitalTimesSampler:

    def __init__(self):
        self.weights = [1.0] * (24 * 60)
        # make events less likely to happen outside waking hours, by
        # tailing off in the evening, and ramping up in the morning.
        falloff_factor = 20.0 # manually tuned
        for minute in range(18 * 60, 24 * 60):
            self.weights[minute] = min(1.0, 1.0 / ((minute - (18*60) + 1) / falloff_factor))
        for minute in range(0, 6 * 60):
            self.weights[(6 * 60) - minute - 1] = min(1.0, 1.0 / ((minute + 1) / falloff_factor))

    def sample(self):
        minute = random.choices(range(0, 24*60), self.weights)[0]
        return datetime.time(hour = minute//60, minute = minute % 60)

    def maybe_sample_after(self, time, limit=5):
        for _ in range(0, limit):
            t = combine_date_time(time.date(), self.sample())
            if t > time:
                return t
        return None

def combine_date_time(date, time):
    return datetime.datetime(date.year, date.month, date.day, time.hour, time.minute, time.second)

def generate_admit_discharge_events(people, times, days, mean_daily_admits):
    next = queue.PriorityQueue()
    start = datetime.datetime(2024, 1, 1)
    admit_distribution = statistics.NormalDist(mean_daily_admits, 10)
    end = start + datetime.timedelta(days=days+1)
    next.put((start, (EVENT_DAY_START, None, None)))
    now = None
    inpatients = set()
    while now is None or now < end:
        now, (event, mrn, detail) = next.get()
        yield (now, (event, mrn, detail))
        if event == EVENT_DAY_START:
            next.put((now + datetime.timedelta(days=1), (EVENT_DAY_START, None, None)))
            for _ in range(0, int(admit_distribution.samples(1)[0])):
                person = None
                while True:
                    person = random.choice(people)
                    if not person.mrn in inpatients:
                        break
                inpatients.add(person.mrn)
                admit_time = combine_date_time(now.date(), times.sample())
                next.put((admit_time, (EVENT_ADMIT, person.mrn, None)))
                discharge_time = admit_time
                while discharge_time <= admit_time:
                    days = int(STAY_DAYS_DISTRIBUTION.samples(1)[0])
                    discharge_time = combine_date_time(now.date() + datetime.timedelta(days=days), times.sample())
                next.put((discharge_time, (EVENT_DISCHARGE, person.mrn, None)))
        elif event == EVENT_DISCHARGE:
            inpatients.remove(mrn)

DAILY_BLOOD_TEST_PROBABILITY = 0.1
ADMIT_BLOOD_TEST_PROBABILITY = 0.4
REPEATED_BLOOD_TEST_PROBABILITY = 0.4

def add_blood_test_events(events, times, by_mrn):
    next = queue.PriorityQueue()
    inpatients = set()
    for e in events:
        now, (event, mrn, detail) = e
        while not next.empty():
            q_now, (q_event, q_mrn, q_detail) = next.get()
            if q_now < now:
                if q_mrn in inpatients:
                    person = by_mrn[q_mrn]
                    result = random.gauss(person.creatinine_mu, person.creatinine_sigma) * person.creatinine_multiplier
                    yield (q_now, (q_event, q_mrn, result))
                    if random.random() < REPEATED_BLOOD_TEST_PROBABILITY:
                        test_time = times.maybe_sample_after(q_now)
                        if test_time:
                            next.put((test_time, (EVENT_BLOOD_TEST, q_mrn, None)))
            else:
                next.put((q_now, (q_event, q_mrn, q_detail)))
                break
        yield (now, (event, mrn, detail))
        if event == EVENT_DAY_START:
            for inpatient in inpatients:
                if random.random() < DAILY_BLOOD_TEST_PROBABILITY:
                    test_time = times.maybe_sample_after(now)
                    if test_time:
                        next.put((test_time, (EVENT_BLOOD_TEST, inpatient, None)))
        elif event == EVENT_ADMIT:
            inpatients.add(mrn)
            if random.random() < ADMIT_BLOOD_TEST_PROBABILITY:
                test_time = times.maybe_sample_after(now)
                if test_time:
                    next.put((test_time, (EVENT_BLOOD_TEST, mrn, None)))
        elif event == EVENT_DISCHARGE:
            inpatients.remove(mrn)

# The age independent probability of a given blood test result indicating
# AKI
BASE_AKI_PROBABILTY = 0.05

# How likely age bands are to suffer AKI, relative to each other.
# From: https://public.tableau.com/app/profile/ukkidney/viz/AKIlandingpage/Landingpage
AKI_AGE_FACTORS = [
    ((0, 18), 0.028 / 0.218),
    ((18, 40), 0.12 / 0.218),
    ((40, 65), 0.29 / 0.218),
    ((65, 75), 0.18 / 0.218),
    ((75, 85), 0.218 / 0.218),
    ((85, 100), 0.16 / 0.218),
]

def inject_aki_blood_test_events(events, by_mrn, epoch, delay):
    start = combine_date_time(epoch + delay, datetime.time(0, 0, 0))
    had_aki = set()
    previous_results = {}
    for e in events:
        now, (event, mrn, detail) = e
        replaced = False
        if event == EVENT_BLOOD_TEST and now > start:
            person = by_mrn[mrn]
            age_factor = 1.0
            age_years = person.age_years(epoch)
            for ((age_begin, age_end), f) in AKI_AGE_FACTORS:
                if age_years >= age_begin and age_years < age_end:
                    age_factor = f
                    break
            else:
                raise AssertionError()
            if len(previous_results.get(mrn, [])) > 0 and not mrn in had_aki and random.random() < BASE_AKI_PROBABILTY * age_factor:
                result = population.choose_creatinine_for_aki(person, previous_results, epoch)
                yield (now, (EVENT_BLOOD_TEST_AKI, mrn, result))
                replaced = True
                had_aki.add(mrn)
            else:
                previous_results.setdefault(mrn, []).append(detail)
        if not replaced:
            yield (now, (event, mrn, detail))

def wait_for_inpatients(events, n):
    events = iter(events)

def wait_until(events, date):
    history = []
    events = iter(events)
    for e in events:
        now, (event, mrn, detail) = e
        history.append(e)
        if now.date() >= date:
            break
    return history, events

def collapse_admits(events):
    inpatients = {}
    for e in events:
        now, (event, mrn, detail) = e
        if event == EVENT_ADMIT:
            inpatients[mrn] = now
        elif event == EVENT_DISCHARGE:
            del inpatients[mrn]
    admits = [(t, (EVENT_ADMIT, mrn, None)) for (mrn, t) in inpatients.items()]
    admits.sort(key=lambda e: e[0])
    return admits

def time_to_hl7_date(time):
    return time.strftime("%Y%m%d")

def time_to_hl7_datetime(time):
    return time.strftime("%Y%m%d%H%M%S")

def new_hl7_message(time, message_code, trigger_event):
    m = Message(f"{message_code}_{trigger_event}", version="2.5", validation_level=VALIDATION_LEVEL.STRICT)
    m.msh.msh_3 = HL7_APP
    m.msh.msh_4 = HL7_HOSPITAL_NAME
    m.msh.msh_7 = time_to_hl7_datetime(time)
    m.msh.msh_9.msh_9_1 = message_code
    m.msh.msh_9.msh_9_2 = trigger_event
    return m

def add_hl7_pid(m, person, demographics=True):
    pid = m.add_segment("PID")
    pid.pid_1 = "1"
    pid.pid_3 = str(person.mrn)
    if demographics:
        pid.pid_5 = person.name
        pid.pid_7 = time_to_hl7_date(person.birthdate)
        pid.pid_8 = "F" if person.sex == population.SEX_FEMALE else "M"

def admit_to_hl7(e, by_mrn):
    now, (event, mrn, detail) = e
    person = by_mrn[mrn]
    admit = new_hl7_message(now, "ADT", "A01")
    add_hl7_pid(admit, person)
    return admit

def discharge_to_hl7(e, by_mrn):
    now, (event, mrn, detail) = e
    person = by_mrn[mrn]
    admit = new_hl7_message(now, "ADT", "A03")
    add_hl7_pid(admit, person, demographics=False)
    return admit

def blood_test_to_hl7(e, by_mrn):
    now, (event, mrn, detail) = e
    person = by_mrn[mrn]
    result = new_hl7_message(now, "ORU", "R01")

    patient_result = result.add_group("ORU_R01_PATIENT_RESULT")
    patient = patient_result.add_group("ORU_R01_PATIENT")
    add_hl7_pid(patient, person, demographics=False)

    order_observation = patient_result.add_group("ORU_R01_ORDER_OBSERVATION")
    obr = order_observation.add_segment("OBR")
    obr.obr_1 = "1"
    obr.obr_7 = time_to_hl7_datetime(now)

    observation = order_observation.add_group("ORU_R01_OBSERVATION")
    obx = observation.add_segment("OBX")
    obx.obx_1 = "1"
    obx.obx_2 = "SN"
    obx.obx_3 = "CREATININE"
    obx.obx_5 = str(detail)
    return result

def to_hl7(events, by_mrn):
    for e in events:
        now, (event, mrn, detail) = e
        if event == EVENT_ADMIT:
            yield admit_to_hl7(e, by_mrn)
        elif event == EVENT_DISCHARGE:
            yield discharge_to_hl7(e, by_mrn)
        elif event == EVENT_BLOOD_TEST or event == EVENT_BLOOD_TEST_AKI:
            yield blood_test_to_hl7(e, by_mrn)

def output_mllp(events, filename, by_mrn):
    with open(filename, "wb") as w:
        for m in to_hl7(events, by_mrn):
            er7 = m.to_er7() + "\r"
            w.write(b"\x0b")
            w.write(bytes(er7, "ascii"))
            w.write(b"\x1c")
            w.write(b"\x0d")

def csv_result_headers(results_by_mrn):
    n_results = max([len(results) for results in results_by_mrn.values()])
    headers = []
    for i in range(0, n_results):
        headers.append(f"creatinine_date_{i}")
        headers.append(f"creatinine_result_{i}")
    return headers

def csv_results(results):
    row = []
    for date, result in results:
        row.append(date.strftime("%Y-%m-%d %H:%M:%S"))
        row.append(f"{result:.2f}")
    return row

def output_history(events, by_mrn, filename):
    results_by_mrn = {}
    for e in events:
        now, (event, mrn, detail) = e
        if event == EVENT_BLOOD_TEST or event == EVENT_BLOOD_TEST_AKI:
            results_by_mrn.setdefault(mrn, []).append((now, detail))
    with open(filename, "w") as f:
        w = csv.writer(f)
        headers = ["mrn"]
        headers.extend(csv_result_headers(results_by_mrn))
        w.writerow(headers)
        for mrn, results in results_by_mrn.items():
            person = by_mrn[mrn]
            row = [str(person.mrn)]
            row.extend(csv_results(results))
            while len(row) < len(headers):
                row.append("")
            w.writerow(row)

def output_training(events, by_mrn, epoch, filename):
    results_by_mrn = {}
    akis = set()
    for e in events:
        now, (event, mrn, detail) = e
        if mrn in akis:
            continue
        if event == EVENT_BLOOD_TEST or event == EVENT_BLOOD_TEST_AKI:
            results_by_mrn.setdefault(mrn, []).append((now, detail))
            if event == EVENT_BLOOD_TEST_AKI:
                akis.add(mrn)
    with open(filename, "w") as f:
        w = csv.writer(f)
        headers = ["age", "sex", "aki"]
        headers.extend(csv_result_headers(results_by_mrn))
        w.writerow(headers)
        for mrn, results in results_by_mrn.items():
            person = by_mrn[mrn]
            row = [str(person.age_years(epoch)), "f" if person.sex == population.SEX_FEMALE else "m", "y" if mrn in akis else "n"]
            row.extend(csv_results(results))
            while len(row) < len(headers):
                row.append("")
            w.writerow(row)

def build_results(events):
    results = {}
    for e in events:
        now, (event, mrn, detail) = e
        if event in (EVENT_BLOOD_TEST, EVENT_BLOOD_TEST_AKI):
            results.setdefault(mrn, []).append((now, detail))
    return results

def output_aki(events, by_mrn, results, epoch, filename):
    with open(filename, "w") as f:
        w = csv.writer(f)
        headers = ["mrn", "date", "aki", "nhs"]
        w.writerow(headers)
        for e in events:
            now, (event, mrn, detail) = e
            if event in (EVENT_BLOOD_TEST, EVENT_BLOOD_TEST_AKI):
                results.setdefault(mrn, []).append((now, detail))
                aki = event == EVENT_BLOOD_TEST_AKI
                age = by_mrn[mrn].age_years(epoch)
                sex = nhs.SEX_FEMALE if by_mrn[mrn].sex == population.SEX_FEMALE else nhs.SEX_MALE
                nhs_detected = nhs.has_aki(age, sex, results[mrn], now)
                if aki or nhs_detected:
                    w.writerow((str(mrn), now.strftime("%Y-%m-%d %H:%M:%S"), "y" if aki else "n", "y" if nhs_detected else "n"))

def aggregate(filename):
    last_time = None
    begin_time = None
    max_time_delta = datetime.timedelta(seconds=0)
    max_time_delta_t = None
    started = False
    with open(filename, "rb") as f:
        data = f.read()
        for message in data[1:-3].split(b"\x1c\x0d\x0b"):
            m = parse_message(message.decode("ascii"))
            if m.msh.msh_9.msh_9_1.value == "ORU":
                started = True
            if started:
                t = datetime.datetime.strptime(str(m.msh.msh_7.value), "%Y%m%d%H%M%S")
                if begin_time is None:
                    begin_time = t
                if last_time:
                    d = t - last_time
                    if d > max_time_delta:
                        max_time_delta = d
                        max_time_delta_t = t
                last_time = t
    print(f"begin: {begin_time}")
    print(f"end: {last_time}")
    print(f"max time delta: {max_time_delta} at {max_time_delta_t}")

POPULATION_FILENAME = "mye2-persons.csv"
SURNAMES_FILENAME = "surnames.txt"
FEMALE_FORENAMES_FILENAME = "female-forenames.txt"
MALE_FORENAMES_FILENAME = "male-forenames.txt"

MLLP_OUTPUT_FILENAME = "messages.mllp"
HISTORY_OUTPUT_FILENAME = "history.csv"
AKI_OUTPUT_FILENAME = "aki.csv"
TRAINING_OUTPUT_FILENAME = "training.csv"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data")
    parser.add_argument("--history", default=3 * 30, type=int)
    parser.add_argument("--days", default=365, type=int)
    parser.add_argument("--mean_daily_admits", default=30, type=int)
    parser.add_argument("--population_size", default=10000, type=int)
    parser.add_argument("--output", default=".")
    parser.add_argument("--aggregate", default=False, action="store_true")
    flags = parser.parse_args()

    if flags.aggregate:
        return aggregate(flags.mllp)

    epoch = datetime.date(2024, 1, 1)

    population_filename = os.path.join(flags.data, POPULATION_FILENAME)
    surnames_filename = os.path.join(flags.data, SURNAMES_FILENAME)
    male_forenames_filename = os.path.join(flags.data, MALE_FORENAMES_FILENAME)
    female_forenames_filename = os.path.join(flags.data, FEMALE_FORENAMES_FILENAME)

    people = population.generate_people(flags.population_size, epoch, population_filename)
    population.add_names(people, surnames_filename, female_forenames_filename, male_forenames_filename)

    for person, mrn in zip(people, random.sample(range(MIN_MRN, MAX_MRN), len(people))):
        person.mrn = mrn
    by_mrn = {}
    for person in people:
        by_mrn[person.mrn] = person

    times = HospitalTimesSampler()
    adt_events = generate_admit_discharge_events(people, times, flags.days + flags.history, flags.mean_daily_admits)
    all_events = add_blood_test_events(adt_events, times, by_mrn)
    aki_events = list(inject_aki_blood_test_events(all_events, by_mrn, epoch, datetime.timedelta(days=flags.history)))
    historical_events, following_events = wait_until(aki_events, epoch + datetime.timedelta(days=flags.history))
    following_events = list(following_events)
    admit_events = collapse_admits(historical_events)
    print(f"initial admits: {len(admit_events)}")
    print("write: mllp")
    mllp_filename = os.path.join(flags.output, MLLP_OUTPUT_FILENAME)
    output_mllp(admit_events + following_events, mllp_filename, by_mrn)
    print("write: historical events")
    history_filename = os.path.join(flags.output, HISTORY_OUTPUT_FILENAME)
    output_history(historical_events, by_mrn, history_filename)
    print("write: expected aki")
    aki_filename = os.path.join(flags.output, AKI_OUTPUT_FILENAME)
    historical_results = build_results(historical_events)
    output_aki(following_events, by_mrn, historical_results, epoch, aki_filename)
    print("write: training")
    training_filename = os.path.join(flags.output, TRAINING_OUTPUT_FILENAME)
    output_training(aki_events, by_mrn, epoch, training_filename)

if __name__ == "__main__":
    main()
