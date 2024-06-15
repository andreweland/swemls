# Software Engineering for Machine Learning Systems

[Software Engineering for Machine Learning Systems](https://www.andreweland.org/swemls) is a course first taught at [Imperial College, London, in Spring 2024](https://www.imperial.ac.uk/computing/current-students/courses/70102/). It covers the engineering concepts required to build and operate robust and trustworthy machine learning systems, while leaving the theory of models themselves to other courses.

The three month course is structured around a real-world example from the medical domain. Students first design and train a model to predict acute kidney injury from synthetic blood test results, before working in groups to build a system around that model to alert clinicians. Ultimately, the groups deploy their systems in a simulated hospital environment. Successfully operating their system over an extended period of time is a key comonent of their assessment. Throughout the course, we use real data standards (eg [HL7](https://en.wikipedia.org/wiki/Health_Level_7#HL7_Version_2)) and deployment environments (eg [Kubernetes](https://kubernetes.io/)), rather than toy simplifications.

At a high level, the coursework flow is:
* Train a model to detect acute kidney injury from a dataset of synthetic blood test results, supplied as a CSV file ([example spec](https://docs.google.com/document/d/13zLCj8fCinawuR5kUD5NsHrPobnQvhfCMnMCYAdhv4I/edit#heading=h.nhjmk7sgdyby)).
* Use the trained model to build a system that predicts acute kidney injury in real time, using blood test results supplied as [HL7 messages](https://en.wikipedia.org/wiki/Health_Level_7#HL7_Version_2) over an [MLLP connection](https://hl7.skyware-group.com/lib/exe/fetch.php?media=wiki:mllp.pdf).
* Add the functionality necessary to ensure their system is robust in the presence of environmental failures.
* Add the monitoring necessary to ensure groups can detect and respond to system failures.
* Run the system in a simulated environment over a period of two weeks, in the presence of deliberate environmental failures (eg machine restarts, corrupt data).

This repository contains the general tools used to deliver the course, including:
* Scaffolding supplied to students as a base for their implementations (under [scaffolding/](scaffolding/README.md))
* A generator for synthetic blood test results, producing both bulk datasets as CSV files, and realtime data as a stream of HL7 messages (under [generator/](generator/README.md)).
* A simulator for a hospital environment, capable of replaying the HL7 messages produced by the generator, and computing the accuracy and latency metrics of downstream systems detecting acute kidney injury (under [simulator/](simulator/README.md))

The respository doesn't contain the full set of tools necessary to deliver the course, as a portion depends on the technical and academic environment in which the course is delivered. Specifically, it does not include:
* Scripts to create and manage the resources necessary to deploy and mark submissions, including:
    * A continuous integration pipeline for earlier courseowrk submissions, which is necessary to produce the outputs that are assessed.
    * Scripts to create and manage cluster resources (persistent volumes, role based access control etc) for the later courseworks.
* Scripts to mark, and provide feedback, for coursework submissions.
