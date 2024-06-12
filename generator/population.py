import csv
import datetime
import random
import statistics

# AKI demography from:
# https://public.tableau.com/app/profile/ukkidney/viz/AKIICBdemography/Snapshot?:render=false
# Using North Central London ICB
# Percentage by age:
# Under 18: 2.8
# 18-39: 12.2
# 40-64: 29.0
# 65-74: 18.2
# 75-84: 21.8
# 85+: 16.0
#
# Percentage by AKI stage at start:
# 1: 82
# 2: 9.3
# 3: 8.8
#
# Percentage by IMD quintile:
# 1 (least): 20
# 2: 30.3
# 3: 21.4
# 4: 19.5
# 5: 8.8
#
# Population
# data/mye2-persons.csv from:
# https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/estimatesofthepopulationforenglandandwales

POPULATION_GEOGRAPHY_COLUMN = 2
POPULATION_AGE_0_COLUMN = 4
POPULATION_LONDON_BOROUGH = "London Borough"

SEX_MALE = 0
SEX_FEMALE = 1

class Person:

    def __init__(self):
        self.birthdate = None
        self.sex = SEX_FEMALE
        self.creatinine_mu = 0
        self.creatinine_sigma = 0
        self.creatine_multiplier = 1.0
        self.creatinine_results = []
        self.aki = False

    def age_years(self, epoch):
        return int((epoch - self.birthdate).days / 365)

def _choose_age(i, by_age):
    for (age, count) in enumerate(by_age):
        if i < count:
            return age
        else:
            i -= count

# Distribution from https://www.researchgate.net/publication/8013307_Distribution_and_determinants_of_serum_creatinine_in_the_general_population_The_Hordaland_Health_Study/link/02bfe511cf35b32bd0000000/download
# umol/L
# ((age_begin, age_end), mu, sigma)

CREATININE_MIN = {
    SEX_MALE: 68.0,
    SEX_FEMALE: 58.0,
}

CREATININE_DISTRIBUTIONS = {
    SEX_MALE: [
        ((0, 50), 100, (130.0 - 100.0) / 3.0),
        ((50, 100), 105, (140.0 - 105.0) / 3.0),
    ],
    SEX_FEMALE: [
        ((0, 50), 80, (110.0 - 80.0) / 3.0),
        ((50, 100), 85, (115.0 - 85.0) / 3.0),
    ],
}

# The NHS reference AKI detection algorithm, described here:
# https://www.england.nhs.uk/wp-content/uploads/2014/06/psa-aki-alg.pdf
# basically alerts on an increase of 2.0
# We introduce false negatives by saying ratio should be lower for older people,
# and false positives by saying ration should be higher for young adults people.
# This isn't intended to be clinically accurate, it's just to introduce a signal
# for machine learning models to find.

AKI_CREATININE_MULTIPLIERS = [
    ((0, 18), 2.0, 2.6),
    ((18, 30), 2.5, 3.5),
    ((30, 40), 2.0, 2.1),
    ((40, 65), 2.0, 2.1),
    ((65, 75), 1.8, 2.1),
    ((75, 85), 1.7, 2.1),
    ((85, 100), 1.5, 2.1),
]

BASELINE_CREATININE_MULTIPLIERS = [
    ((0, 18), 1.0, 1.0),
    ((18, 30), 1.0, 2.5),
    ((30, 100), 1.0, 1.0),
]

def choose_creatinine_for_aki(person, previous_results, epoch):
    age_years = person.age_years(epoch)
    for ((age_begin, age_end), low, high) in AKI_CREATININE_MULTIPLIERS:
        if age_years >= age_begin and age_years < age_end:
            factor = random.uniform(low, high)
    return statistics.median(previous_results) * factor

def _choose_creatinine_baseline(person, epoch):
    creatinine_multipler = 1.0
    age_years = person.age_years(epoch)
    for ((age_begin, age_end), low, high) in BASELINE_CREATININE_MULTIPLIERS:
        if age_years >= age_begin and age_years <= age_end:
            creatinine_multipler = random.uniform(low, high)
    for ((age_begin, age_end), mu, sigma) in CREATININE_DISTRIBUTIONS[person.sex]:
        if age_years >= age_begin and age_years <= age_end:
            creatinine_mu = random.gauss(mu, sigma)
            if creatinine_mu < CREATININE_MIN[person.sex]:
                creatinine_mu = CREATININE_MIN[person.sex]
            return (creatinine_mu, sigma, creatinine_multipler)
    raise Exception("No valid creatinine distribution")

# Generate a random sample of n people, with an age distribution at the epoch
# that matches that seen across the London boroughs, according to the census
# age distribution data passed as ages_filename.
def generate_people(n, epoch, ages_filename):
    r = csv.reader(open(ages_filename))
    by_age = []
    for row in r:
        if len(row) > POPULATION_GEOGRAPHY_COLUMN + 1:
            if row[POPULATION_GEOGRAPHY_COLUMN] == POPULATION_LONDON_BOROUGH:
                by_age.extend([0] * (len(row[POPULATION_AGE_0_COLUMN:]) - len(by_age)))
                for (age, count) in enumerate(row[POPULATION_AGE_0_COLUMN:]):
                    by_age[age] += int(count.replace(",", ""))
    total_people = sum(by_age)
    people = []
    for _ in range(0, n):
        person = Person()
        age_years = _choose_age(random.randrange(0, total_people), by_age)
        age_days = random.randrange(0, 365)
        person.birthdate = datetime.date(epoch.year - age_years, epoch.month, epoch.day) - datetime.timedelta(days=age_days)
        person.sex = random.choice([SEX_MALE, SEX_FEMALE])
        (person.creatinine_mu, person.creatinine_sigma, person.creatinine_multiplier) = _choose_creatinine_baseline(person, epoch)
        people.append(person)
    return people

def add_names(people, surnames_filename, female_forenames_filename, male_forenames_filename):
    surnames = [l.strip() for l in open(surnames_filename)]
    female_forenames = [l.strip() for l in open(female_forenames_filename)]
    male_forenames = [l.strip() for l in open(male_forenames_filename)]
    for p in people:
        surname = random.choice(surnames)
        forename = random.choice(female_forenames if p.sex == SEX_FEMALE else male_forenames)
        # Ensure name is 7bit ASCII safe for HL7
        p.name = "".join([c for c in forename + " " + surname if ord(c) < 128])