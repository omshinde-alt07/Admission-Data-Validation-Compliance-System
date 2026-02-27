# Prompt 01 – Foundation (Workflow Design & Improvement)

## Context

We are building an automated **Candidate Screening System** that collects applicant data, validates it, processes it, and generates a shortlist for interviews.

---

## Objective

Design and improve a workflow that:

* Collects candidate data through a web form
* Stores responses in Google Sheets
* Cleans and validates the data using Python
* Generates analytics dashboards
* Automatically shortlists candidates based on screening performance
* Produces a final interview-ready dataset

The system should be scalable, easy to maintain, and require minimal manual work.

---

## Current System Flow

### 1. Data Collection

Create a web-based form with good UI to collect:

* Name
* Email
* Phone
* Education
* Experience
* Applied Role
* Screening Test Score (if applicable)

Store responses directly into **Google Sheets** using a **Google Service Account**.
This sheet acts as the **Raw Data Layer**.

---

### 2. Data Processing (Python)

Build a Python script that:

* Reads raw data from Google Sheets
* Performs validation:

  * Mandatory fields check
  * Email format validation
  * Phone format validation
  * Duplicate detection/removal
  * Score range validation
* Cleans and standardizes data
* Writes output to a **Clean Data Sheet**

This becomes the **Processed Data Layer**.

---

### 3. Dashboard & Analysis

Create dashboards using the Clean Data Sheet:

Metrics:

* Total applicants
* Role-wise distribution
* Average screening scores
* Pass vs Fail ratio

Dashboard Options:

* Google Sheets Charts
* Looker Studio
* Simple Web Dashboard (optional)

---

### 4. Screening & Shortlisting

A second Python process should:

* Map screening scores with candidate data
* Apply selection rules:

  * Minimum score threshold
  * Role-specific criteria
* Generate a **Shortlisted Candidates Sheet**

---

### 5. Final Output

Provide an **Interview Panel Sheet** containing:

* Candidate details
* Screening score
* Selection Status:

  * Selected
  * Rejected

---

## Tool Research Requirement

Suggest modern tools that provide better UI than basic forms and support **direct Google Sheets integration**, such as:

* No-code form builders
* No-code website builders
* Tools with modern UI and easy integration

Examples to evaluate:

* Tally
* Typeform
* Jotform
* Glide
* Softr
* Other relevant tools

---

## Workflow Analysis Requirement

Analyze the above system and suggest improvements in the following areas:

### Architecture Improvements

* Better data flow design
* Layer separation (Raw → Clean → Final)
* Automation triggers (scheduled / event-based)

### Data Quality Improvements

* Edge case handling
* Exception handling (e.g., borderline scores like 59.5%)
* Status tracking (Pending / Approved / Rejected)

### Scalability

* Handling large datasets
* Reducing manual intervention
* Config-based validation instead of hardcoding rules

### Reliability

* Logging and error tracking
* Audit trail for changes
* Duplicate prevention at form level

### User Experience

* Better candidate form experience
* Admin review interface for exceptions
* Dashboard usability improvements

---

