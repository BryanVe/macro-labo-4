CREATE TABLE symptom_severities (
    symptom VARCHAR PRIMARY KEY,
    weight INT NOT NULL
);

CREATE TABLE disease_precautions (
    disease VARCHAR PRIMARY KEY,
    precaution1 VARCHAR,
    precaution2 VARCHAR,
    precaution3 VARCHAR,
    precaution4 VARCHAR
);

CREATE TABLE disease_descriptions (
    disease VARCHAR PRIMARY KEY,
    description VARCHAR
);

CREATE TABLE disease_symptoms(
    disease VARCHAR,
    symptom1 VARCHAR,
    symptom2 VARCHAR,
    symptom3 VARCHAR,
    symptom4 VARCHAR,
    symptom5 VARCHAR,
    symptom6 VARCHAR,
    symptom7 VARCHAR,
    symptom8 VARCHAR,
    symptom9 VARCHAR,
    symptom10 VARCHAR,
    symptom11 VARCHAR,
    symptom12 VARCHAR,
    symptom13 VARCHAR,
    symptom14 VARCHAR,
    symptom15 VARCHAR,
    symptom16 VARCHAR,
    symptom17 VARCHAR
);